CUSTOM_INCLUDE = /opt/homebrew/Cellar/sqlite/3.46.0/include
CUSTOM_LIB = /opt/homebrew/Cellar/sqlite/3.46.0/lib

skylite.dylib: skylite_ext.c skylite_ext.a
	#gcc -g -fPIC -shared -o $@ $^ -framework CoreFoundation -framework Security
	gcc -dynamiclib -o $@ $^ -I$(CUSTOM_INCLUDE) -L$(CUSTOM_LIB) -lsqlite3 -framework CoreFoundation -framework Security

skylite_ext.a: skylite_ext.go
	go build -tags SQLITE3VFS_LOADABLE_EXT -o $@ -buildmode=c-archive $^
	rm -f skylite_ext.h

.PHONY: clean
clean:
	rm -f skylite_ext.a skylite.dylib
