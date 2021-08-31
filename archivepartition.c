#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <mysql.h>

/**
 * I have not been able to find any documentation on it but whenever I try to allocate the MYSQL object
 * on the stack and use mysql_init(&mysql), other data on the stack gets corrupted. Switching the MYSQL object to
 * a pointer and using mysql = mysql_init(NULL) works fine. I'm not sure what the issue is with allocating the
 * MySQL object on the stack inside a UDF.
 */

#define RESULT_MAX_LEN (255 - 1)

typedef struct {
    unsigned int port;
    const char *socket;
} config_t;

typedef struct {
    const char *user;
    const char *password;
    const char *database;
    const char *table;
    const char *partition;
    const char *data_directory;
} params_t;

static void
args_to_params(UDF_ARGS *args, params_t *params) {
    params->user = args->args[0];
    params->password = args->args[1];
    params->database = args->args[2];
    params->table = args->args[3];
    params->partition = args->args[4];
    params->data_directory = args->args[5];
}

static config_t config = {
    .port = 3306,
    .socket = "/var/run/mysqld/mysqld.sock"
};

static size_t
strlcpy(char *dst, const char *src, size_t size) {
    const char *orig;
    size_t left;

    orig = src;
    left = size;

    if (left != 0) {
        while (--left != 0) {
            if ((*dst++ = *src++) == '\0') {
                break;
            }
        }
    }

    if (left == 0) {
        if (size != 0) {
            *dst = '\0';
        }
        while (*src++);
    }

    return src - orig - 1;
}

/**
 * Get a MySQL variable.
 *
 * @param[in] mysql The MySQL context. 
 * @param[in] name The name of the variable.
 * @param[out] value The value of the variable 'name'.
 * @param[in] value_size The size of the array 'value'.
 * @param[out] value_len The string length of the array 'value'.
 * @param[out] error MySQL's UDF error array to store an error message in, if one occurs.
 * @return true on success, otherwise false.
 */
static bool
get_variable(MYSQL *mysql, const char *name, char *value, size_t value_size, size_t *value_len, char *error, unsigned long *error_len) {
    MYSQL_RES *res;
    MYSQL_ROW row;
    bool success = false;
    char query[256];
    int len;

    len = snprintf(query, sizeof(query), "SHOW VARIABLES WHERE `Variable_name`='%s'", name);

    if (mysql_real_query(mysql, query, len) != 0) {
        *error_len = snprintf(error, RESULT_MAX_LEN, "Error getting variable '%s': %s", name, mysql_error(mysql));
        return false;
    }

    res = mysql_store_result(mysql);
    if (res == NULL) {
        *error_len = snprintf(error, RESULT_MAX_LEN, "Error getting variable '%s': %s", name, mysql_error(mysql));
        return false;
    }

    row = mysql_fetch_row(res);
    if (row == NULL) {
        *error_len = snprintf(error, RESULT_MAX_LEN, "Variable '%s' not found", name);
    }
    else {
        *value_len = strlcpy(value, row[1], value_size);
        success = true;
    }

    mysql_free_result(res);
    return success;
}

static bool
validate_partition(MYSQL *mysql, params_t *params, char *error, unsigned long *error_len) {
    MYSQL_RES *res;
    MYSQL_ROW row;
    char query[128];
    int len, count = 0;

    len = snprintf(query, sizeof(query), "SELECT COUNT(*)\n"
                                         "FROM `information_schema`.`PARTITIONS`\n"
                                         "WHERE `TABLE_NAME`='%s'\n"
                                         "AND `PARTITION_NAME`='%s'",
                                         params->table, params->partition);

    if (mysql_real_query(mysql, query, len) != 0) {
        *error_len = snprintf(error, RESULT_MAX_LEN, "Error validating partition: %s", mysql_error(mysql));
        return false;
    }

    res = mysql_store_result(mysql);
    if (res == NULL) {
        *error_len = snprintf(error, RESULT_MAX_LEN, "Error validating partition: %s", mysql_error(mysql));
        return false;
    }

    row = mysql_fetch_row(res);
    count = atoi(row[0]);
    mysql_free_result(res);

    if (count == 0) {
        *error_len = strlcpy(error, "Partition not found", RESULT_MAX_LEN);
        return false;
    }

    return true;
}

static bool
get_file_from(MYSQL *mysql, params_t *params, char *file_from, size_t file_from_size, char *error, unsigned long *error_len) {
    bool success;
    char dir[2048];
    size_t dir_len;

    success = get_variable(mysql, "datadir", dir, sizeof(dir), &dir_len, error, error_len);
    if (!success) {
        return false;
    }

    //remove a trailing slash if it has one
    if (dir[dir_len - 1] == '/') {
        dir[dir_len - 1] = '\0';
    }

    snprintf(file_from, file_from_size, "%s/%s/%s#P#%s.ibd", dir, params->database, params->table, params->partition);

    return true;
}

static bool
copy_partition(const char *file_from, const char *file_to, char *error, unsigned long *error_len) {
    int fd_from = -1, fd_to = -1;
    bool success = false;
    struct stat st;
    off_t left;
    ssize_t n;

    fd_from = open(file_from, O_RDONLY);
    if (fd_from == -1) {
        *error_len = snprintf(error, RESULT_MAX_LEN, "Error copying partition: Opening source: %s", strerror(errno));
        return false;
    }

    if (fstat(fd_from, &st) == -1) {
        *error_len = snprintf(error, RESULT_MAX_LEN, "Error copying partition: Stat source: %s", strerror(errno));
        goto done;
    }

    fd_to = open(file_to, O_WRONLY | O_CREAT, st.st_mode);
    if (fd_to == -1) {
        *error_len = snprintf(error, RESULT_MAX_LEN, "Error copying partition: Opening destination: %s", strerror(errno));
        goto done;
    }

    left = st.st_size;
    while (left > 0) {
        n = sendfile(fd_to, fd_from, NULL, left);

        if (n == -1) {
            *error_len = snprintf(error, RESULT_MAX_LEN, "Error copying partition: %s", strerror(errno));
            goto done;
        }

        left -= n;
    }

    success = true;

done:
    if (fd_from != -1) {
        close(fd_from);
    }
    if (fd_to != -1) {
        close(fd_to);
        if (!success) {
            remove(file_to);
        }
    }

    return success;
}

/**
 * Calling convention is MOVE_PARTITION('user', 'password', 'database', 'table', 'partition', 'new data directory')
 */

my_bool
move_partition_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    params_t params;
    struct stat st;
    int i;

    if (args->arg_count != 6) {
        strlcpy(message, "Expected 6 parameters", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    for (i = 0; i < 6; i++) {
        if (args->arg_type[i] != STRING_RESULT) {
            snprintf(message, MYSQL_ERRMSG_SIZE, "Parameter %d must be a string", i + 1);
            return 1;
        }
    }

    args_to_params(args, &params);

    if (stat(params.data_directory, &st) != 0) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "'%s': %s", params.data_directory, strerror(errno));
        return 1;
    }

    if (!S_ISDIR(st.st_mode)) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "'%s' is not a directory", params.data_directory);
        return 1;
    }

    if (access(params.data_directory, W_OK) != 0) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "'%s' is not not writable", params.data_directory);
        return 1;
    }

    initid->max_length = RESULT_MAX_LEN;

    //the result cannot be NULL
    initid->maybe_null = 0;

    //the result is not constant
    initid->const_item = 0;

    return 0;
}

void
move_partition_deinit(UDF_INIT *initid) {
    params_t *params;

    if (initid->ptr != NULL) {
        params = (params_t *)initid->ptr;
        free(params);
    }
}

char *
move_partition(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, char *is_null, char *error) {
    char query[256], file_from[2048 + 256], dir_to[2048], file_to[2048 + 256];
    MYSQL* mysql;
    params_t params;
    int len;

    *is_null = 0;
    *error = 0;

    args_to_params(args, &params);

    mysql = mysql_init(NULL);
    if (mysql == NULL) {
        *length = strlcpy(result, "Out of memory", RESULT_MAX_LEN);
        goto done;
    }

    if (mysql_real_connect(mysql, NULL, params.user, params.password, params.database, config.port, config.socket, 0) == NULL) {
        *length = snprintf(result, RESULT_MAX_LEN, "Error connecting to MySQL: %s", mysql_error(mysql));
        goto done;
    }

    if (!validate_partition(mysql, &params, result, length)) {
        goto done;
    }

    if (!get_file_from(mysql, &params, file_from, sizeof(file_from), result, length)) {
        goto done;
    }

    snprintf(dir_to, sizeof(dir_to), "%s/%s", params.data_directory, params.database);
    snprintf(file_to, sizeof(file_to), "%s/%s#P#%s.ibd", dir_to, params.table, params.partition);

    //create <data directory>/<database> if it doesn't exist
    if (access(dir_to, F_OK) != 0) {
        if (mkdir(dir_to, 0700) != 0) {
            *length = snprintf(result, RESULT_MAX_LEN, "Error creating data directory database folder: %s", strerror(errno));
            goto done;
        }
    }

    len = snprintf(query, sizeof(query), "LOCK TABLE `%s` WRITE", params.table);
    if (mysql_real_query(mysql, query, len) != 0) {
        *length = snprintf(result, RESULT_MAX_LEN, "Error getting write lock: %s", mysql_error(mysql));
        goto done;
    }

    if (rename(file_from, file_to) != 0) {
        if (errno == EXDEV) {
            //tried to move a file across mount points, use sendfile instead()
            if (!copy_partition(file_from, file_to, result, length)) {
                goto unlock;
            }
        }
        else {
            *length = snprintf(result, RESULT_MAX_LEN, "Error moving partition: %s", strerror(errno));
            goto unlock;
        }
    }

    if (symlink(file_to, file_from) != 0) {
        *length = snprintf(result, RESULT_MAX_LEN, "Error creating symbolic link: %s", strerror(errno));
        goto unlock;
    }

    *length = strlcpy(result, "OK", RESULT_MAX_LEN);

unlock:
    len = snprintf(query, sizeof(query), "UNLOCK TABLES");
    mysql_real_query(mysql, query, len);

done:
    if (mysql != NULL) {
        mysql_close(mysql);
    }

    return result;
}
