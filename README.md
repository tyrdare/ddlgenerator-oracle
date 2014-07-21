ddlgenerator-oracle
===================

Oracle ddl generator - puts a python command line on the dbms_metadata.get_ddl function.

"python ddl_generator.py -h" 

will tell you something useful.

Dependencies:
============
cx_Oracle
Oracle client libraries installation


TODO:
=====
- Add an "all" modifier on object options
- Add a "get everything at once" option.
- Allow concurrent processes for large schemas.  dbms_metadata.get_ddl is not the fastest thing in the world...
