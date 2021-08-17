lib=lib_mysqludf_archivepartition.so

obj=archivepartition.o

cc=gcc
cflags=`mariadb_config --cflags` -D_GNU_SOURCE -fPIC -Wall -g
ldflags=`mariadb_config --libs` -shared

all: $(lib)

$(lib): $(obj)
	$(cc) -o $@ $^ $(ldflags)

%.o: %.c
	$(cc) -o $@ -c $< $(cflags)

install: $(lib)
	@mv $(lib) `mariadb_config --plugindir`
	@echo "Installing function in MySQL"
	@mysql -u $(USER) -p -e "CREATE OR REPLACE FUNCTION move_partition RETURNS string SONAME '$(lib)'"

clean:
	rm -f $(obj) $(lib)
