//go:build SQLITE3VFS_LOADABLE_EXT
// +build SQLITE3VFS_LOADABLE_EXT

package main

// import C is necessary for us to export in the c-archive .a file

import "C"
import (
	"fmt"
)

func main() {
}

//export S3qliteRegister
func S3qliteRegister() {
	fmt.Printf("Registering s3qlite vfs\n")
	//vfs := s3qlite.NewVFS()
	//
	//err := sqlite3vfs.RegisterVFS("skylite", vfs)
	//if err != nil {
	//	fmt.Printf("register vfs err: %s\n", err)
	//	panic(err)
	//	//log.Fatalf("register vfs err: %s", err)
	//}
	fmt.Printf("Registered s3qlite vfs\n")
}
