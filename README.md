# Skylite VFS

## Overview

Skylite VFS is a custom Virtual File System (VFS) for SQLite, designed for embedding in distributed microservices. It provides a way to use SQLite databases with an object store such as S3 as the backend storage and etcd for transaction coordination.

### Status

Incomplete right now. This currently stores pages locally with BoltDB. It's missing the etcd coordination and S3 storage.

