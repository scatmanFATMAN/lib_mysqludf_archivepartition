#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <mysql.h>

/**
 * I have not been able to find any documentation on it but apparently you CANNOT call mysql_init() in the
 * _init() functions. Anytime I tried to, it'd overwrite 2 pointer sizes after the MYSQL structure and things
 * would crash. Moving it out of the _init() function works just fine.
 */

#define RESULT_MAX_LEN 255

typedef struct {
    const char *user;
    const char *password;
    const char *database;
    const char *table;
    const char *partition;
    const char *data_directory;
    char file_from[2048 + 256];
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

static bool
validate_partition(MYSQL *mysql, params_t *params, char *message) {
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
        snprintf(message, MYSQL_ERRMSG_SIZE, "Error validating partition: %s", mysql_error(mysql));
        return false;
    }

    res = mysql_store_result(mysql);
    row = mysql_fetch_row(res);
    count = atoi(row[0]);
    mysql_free_result(res);

    if (count == 0) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "Error validating partition: Partition not found");
        return false;
    }

    return true;
}

static bool
get_from_partition_path(MYSQL *mysql, params_t *params, char *message) {
    MYSQL_RES *res;
    MYSQL_ROW row;
    bool success = false;
    char query[128], dir[2048];
    int len;

    len = snprintf(query, sizeof(query), "SHOW VARIABLES WHERE Variable_name='datadir'");

    if (mysql_real_query(mysql, query, len) != 0) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "Error getting data directory: %s", mysql_error(mysql));
        return false;
    }

    res = mysql_store_result(mysql);
    row = mysql_fetch_row(res);
    if (row == NULL) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "Error getting data directory: Variable 'datadir' not found");
    }
    else {
        strcpy(dir, row[1]);
        len = strlen(dir);;
        if (dir[len - 1] == '/') {
            dir[len - 1] = '\0';
        }
        snprintf(params->file_from, sizeof(params->file_from), "%s/%s/%s#P#%s.ibd", dir, params->database, params->table, params->partition);
        success = true;
    }

    mysql_free_result(res);
    return success;
}

/**
 * Calling convention is MOVE_PARTITION('user', 'password', 'database', 'table', 'partition', 'data directory')
 */

my_bool
move_partition_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    params_t params;
    struct stat st;
    int i;

    if (args->arg_count != 6) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "Incorrect parameter count; Expected 6");
        return 1;
    }

    for (i = 0; i < 5; i++) {
        if (args->arg_type[0] != STRING_RESULT) {
            snprintf(message, MYSQL_ERRMSG_SIZE, "Parameter %d must be a string", i);
            return 1;
        }
    }

    args_to_params(args, &params);

    if (stat(params.data_directory, &st) != 0) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "Failed to check data directory '%s': %s", params.data_directory, strerror(errno));
        return 1;
    }

    if (!S_ISDIR(st.st_mode)) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "Failed to check data directory '%s': Not a directory", params.data_directory);
        return 1;
    }

    if (access(params.data_directory, W_OK) != 0) {
        snprintf(message, MYSQL_ERRMSG_SIZE, "Failed to check data directory '%s': Not writable", params.data_directory);
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

    //    mysql_close(&params->mysql);
        free(params);
    }
}

char *
move_partition(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, char *is_null, char *error) {
    char query[256], dir_to[2048], file_to[2048 + 256];
    MYSQL* mysql;
    params_t params;
    int len;

    *is_null = 0;
    *error = 0;

    args_to_params(args, &params);
    mysql = mysql_init(NULL);

    if (mysql_real_connect(mysql, NULL, params.user, params.password, params.database, 3306, "/var/run/mysqld/mysqld.sock", 0) == NULL) {
        *length = snprintf(result, RESULT_MAX_LEN, "Error connecting to MySQL: %s", mysql_error(mysql));
        goto done;
    }

    if (!validate_partition(mysql, &params, result)) {
        goto done;
    }

    if (!get_from_partition_path(mysql, &params, result)) {
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

    if (rename(params.file_from, file_to) != 0) {
        *length = snprintf(result, RESULT_MAX_LEN, "Error moving partition: %s", strerror(errno));
        goto unlock;
    }

    if (symlink(file_to, params.file_from) != 0) {
        *length = snprintf(result, RESULT_MAX_LEN, "Error creating symbolic link: %s", strerror(errno));
        goto unlock;
    }

    *length = snprintf(result, RESULT_MAX_LEN, "OK");

unlock:
    len = snprintf(query, sizeof(query), "UNLOCK TABLES");
    mysql_real_query(mysql, query, len);

done:
    mysql_close(mysql);

    return result;
}
