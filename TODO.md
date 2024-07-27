- [ ] The firstPage template stuff is potentially unnecessary and confusing to sqlite. 
Should test sqlite behavior on empty file systems and see if we can get it to provide the
first pages for us.
- [ ] This is working as a dynamic library but has some strange locking problems in JDBC. We
can factor out psanford code and build the C ABI ourselves.
- [ ] Debugging is pretty tough with the runtimes involved. Need some sort of consistent mode 
where we share symbols and have figured out a way to hook a debugger into the different layers
- [ ] File and VFS are tightly coupled. Can address when psanford lib is factored out.
- [ ] Truncate and filesize are not implemented accurately
- [ ] Rename fully to skylite
- [ ] Cross-platform building. Should do after ABI refactor.
- [ ] Read-open and exclusive flags for db file