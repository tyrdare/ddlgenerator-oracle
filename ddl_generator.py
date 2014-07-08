import argparse
import cx_Oracle
import os
import sys
import re
import ddlexceptions

class DDLGenerator(object):
    # Class variables
    # mapping of object types that show up in the user_objects view to the types used in dbms_metadata.get_ddl() function
    object_types = {
            'DATABASE LINK': 'DB_LINK',
            'FUNCTION': 'FUNCTION',
            'INDEX':'INDEX',
            'PACKAGE':'PACKAGE', 'PACKAGE BODY': 'PACKAGE', 'PROCEDURE':'PROCEDURE',
            'SEQUENCE': 'SEQUENCE', 'SYNONYM':'SYNONYM',
            'TABLE':'TABLE', 'TRIGGER':'TRIGGER', 'TYPE': 'TYPE',
            'VIEW': 'VIEW',
    }
    allowed_object_types = [
        'DB_LINK','FUNCTION','INDEX','PACKAGE','PROCEDURE', 'SEQUENCE','SYNONYM','TABLE','TRIGGER','VIEW'
    ]
    # database function names
    grants_ddl_func = 'dbms_metadata.get_grant_ddl'

    #-----------------------------------------
    def __init__(self, arg_obj):

        # argparse object
        self.args = arg_obj
        # Database connection
        self.conn = None

        self.schema = None

        # object type l
        # lists (TABLE, SEQ,...,ALL
        self.all = None
        self.tables = None
        self.views = None
        self.procs = None
        self.funcs = None
        self.trigs = None
        self.dblinks = None
        self.seqs = None
        self.syns = None
        self.idxs = None
        self.pkgs = None
        self.object_names = {
            'TABLE': list(), 'VIEW': list(), 'PROCEDURE': list(),
            'FUNCTION': list(),'TRIGGER': list(),'DB_LINK': list(),
            'SEQUENCE': list(),'SYNONYM': list(),'INDEX': list(),
            'PACKAGE': list(),
        }
        # This is used to index into the object_names dictionary
        # holds a list of the object types specified on the command line.
        # However, if no object names where specified on the command line (e.g. --all was specified)
        # object_names will remain a bunch of empty lists.
        self.object_types_to_fetch = list()

        # path to deposit ddl statement file when output type is 'per_object'
        self.output_path = None

        self.validate_args(self.args)

    #-----------------------------------------

    def write_ddl(self, obj_type, obj_name, schema, ddl_str, output_path):
        """
            Writes out the ddl string to a file
        """
        if output_path is not None:
            if output_path is not None:
                # build the filename out of the schema name, object_type, oject_name
                fname = "%s_%s_%s.sql" % (schema, obj_type, obj_name)
                print "Writing file %s" % fname
                f = open(os.path.join(output_path, fname), 'w')
                f.write(str(ddl_str))
                f.close()

    #-----------------------------------------
    def get_ddl(self, conn, schema_name):
        """
            Gets the DDL statements for the specified objects
        """

        object_ddl_func = 'dbms_metadata.get_ddl'
        curs = conn.cursor()
        # go through the list of object types for which we're supposed to fetch ddl
        for obj_type in self.object_types_to_fetch:
            #print obj_type, self.object_names[obj_type]
            if 'ALL' in self.object_names[obj_type]:
                # We're going to get all object of a certain type in the schema
                # So get all object names for that object type. Skip any other names specified.
                o_names = self.get_all_objects_of_type(conn,schema_name,obj_type)
                for o_name in o_names:
                    print "Processing %s %s.%s" % (obj_type.lower(), schema_name, o_name)
                    ddl_str =  curs.callfunc(object_ddl_func, cx_Oracle.CLOB, (obj_type, o_name, schema_name))
                    self.write_ddl(obj_type, o_name, schema_name, ddl_str, self.output_path)
            else:
                # Getting objects of a certain type having a name specified by the user
                for obj_name in self.object_names[obj_type]:
                    print "Processing %s %s.%s" % (obj_type.lower(), schema_name, obj_name)
                    ddl_str =  curs.callfunc(object_ddl_func, cx_Oracle.CLOB, (obj_type, obj_name, schema_name))
                    self.write_ddl(obj_type,obj_name, schema_name, ddl_str, self.output_path)
        curs.close()

    #-----------------------------------------
    def get_all_objects_of_type(self, conn, schema_name, obj_type):
        """
            Returns a list of names of all objects of a given type
        """
        names = list()
        curs2 = conn.cursor()
        sql = """
            select object_name
            from user_objects
            where object_type = :obj_type
            order by object_name
        """
        if obj_type == "DB_LINK":
            db_obj_type = "DATABASE LINK"
        else:
            db_obj_type = obj_type
        curs2.execute(sql,{'obj_type': db_obj_type})
        for row in curs2:
            names.append(row[0])
        curs2.close()
        return names


    #-----------------------------------------
    def validate_args(self, args):
        """
            Validates command line options
        """
        if args.info:
            self.show_supported_objects()
            sys.exit(0)
    
        self.check_object_args(args,self.object_types_to_fetch, self.object_names)

        self.schema = args.dburl.split("/")[0]

        self.validate_file_option(args)
        self.check_db_url(args)

    #-----------------------------------------
    def objects_are_none(self, args_object):
        """
            determines if no db object arguments were supplied
        """
        # While these are optional parameters, there has to be at least one
        # for it to be worthwhile to continue running the program.
        if (
            (args_object.tables == '' or args_object.tables is None)
            and (args_object.dblinks == '' or args_object.dblinks is None)
            and (args_object.pkgs == '' or args_object.pkgs is None)
            and (args_object.procs == '' or args_object.procs is None)
            and (args_object.funcs == '' or args_object.funcs is None)
            and (args_object.seqs == '' or args_object.seqs is None)
            and (args_object.trigs == '' or args_object.trigs is None)
            and (args_object.views == '' or args_object.views is None)
            and (args_object.syns == '' or args_object.syns is None)
            and (args_object.idxs == '' or args_object.idxs is None)
        ):
            return True
        return False

    #-----------------------------------------
    def set_object_types(self,args_object, object_types_to_fetch, object_names):
                    # Collect the types and names of objects the user wants ddl for
            if args_object.tables is not None:
                object_names['TABLE'] = args_object.tables.upper().split(",")
                object_types_to_fetch.append('TABLE')
            if args_object.dblinks is not None:
                object_names['DB_LINK'] = args_object.dblinks.upper().split(",")
                object_types_to_fetch.append('DB_LINK')
            if args_object.pkgs is not None:
                object_names['PACKAGE'] = args_object.pkgs.upper().split(",")
                object_types_to_fetch.append('PACKAGE')
            if args_object.procs is not None:
                object_names['PROCEDURE'] = args_object.procs.upper().split(",")
                object_types_to_fetch.append('PROCEDURE')
            if args_object.funcs is not None:
                object_names['FUNCTION'] = args_object.funcs.upper().split(",")
                object_types_to_fetch.append('FUNCTION')
            if args_object.seqs is not None:
                object_names['SEQUENCE'] = args_object.seqs.upper().split(",")
                object_types_to_fetch.append('SEQUENCE')
            if args_object.trigs is not None:
                object_names['TRIGGER'] = args_object.trigs.upper().split(",")
                object_types_to_fetch.append('TRIGGER')
            if args_object.views is not None:
                object_names['VIEW'] = args_object.views.upper().split(",")
                object_types_to_fetch.append('VIEW')
            if args_object.syns is not None:
                object_names['SYNONYM'] = args_object.syns.upper().split(",")
                object_types_to_fetch.append('SYNONYM')
            if args_object.idxs is not None:
                object_names['INDEX'] = args_object.idxs.upper().split(",")
                object_types_to_fetch.append('INDEX')

    #-----------------------------------------
    def check_object_args(self, args_object, object_types_to_fetch, object_names):
        """
            checks to see if db object args have been supplied and stores any that have been
        """
        print "check_object_args(): Start"
        if self.objects_are_none(args_object):
            raise ValueError("No database object types specified")
        else:
            self.set_object_types(args_object,object_types_to_fetch, object_names)

        print "Getting database objects that are %s" % ",".join(object_types_to_fetch)


    #-----------------------------------------
    def validate_file_option(self, args):
        """
            Validate the destination where ddl files will be put
        """
        # see if user wants us to generate a file for each object
        if not os.path.exists(args.output_path):
            raise ddlexceptions.DirNoExistError("Invalid path: %s" % args.output_path)

        self.output_path = args.output_path

        print "ddl output destination set to %s" % self.output_path

    #-----------------------------------------
    def show_supported_objects(self):
        """
            show the user the list objects we can return ddl statements for
        """
        print 'The program can generate DDL for the following object types:'
        for obj_type in self.allowed_object_types:
            print "\t%s" % obj_type

    #-----------------------------------------
    def check_db_url(self, args_object):
        """
            check the dburl supplied on the command line
        """
        if args_object.dburl == '' or args_object.dburl is None:
            raise ValueError("No database connection specified")

        # see if the dburl is in the proper format
        url_matcher = re.compile('.+/.+@.+')
        if url_matcher.match(args_object.dburl) is None:
            raise ddlexceptions.BadDbUrlFormatError("DB URL is not in proper format. Should be: 'username/password@dnalias'")

        self.test_db_connection(args_object.dburl)
        self.dburl = args_object.dburl

        print "Database connection set"

    #-----------------------------------------
    def test_db_connection(self, url):
        """
            tries to get a connection to the database.  Kicks an error if it can't
        """
        try:
            conn = cx_Oracle.connect(url)
        except cx_Oracle.DatabaseError as dbe:
            err, = dbe.args
            if err.code == 12154:  #bad db alias
                raise ddlexceptions.BadDbAliasError("Invalid TNS alias. check your tnsnames.ora")
            elif err.code == 1017: # bad username/password
                raise ddlexceptions.BadDbUserCredsError("Invalid Username or Password")
            else:
                raise
        else:
            self.conn = conn

#-----------------------------------------
#!!!!!! Not part of the above class !!!!!!
def get_command_line_args():
    """
        get all the command line options into a single variable
    """
    parser = argparse.ArgumentParser(description="A Program to get Oracle database DDL")
    parser.add_argument('--tables', type=str, help="table name | TABLE1,..,TABLEn")
    parser.add_argument('--dblinks', type=str, help="dblink name | DBL1,...,DBLn")
    parser.add_argument('--pkgs', type=str, help='package name | PKG1,...,PKGn')
    parser.add_argument('--procs', type=str, help='procedure name | PROC1,...,PROCn')
    parser.add_argument('--funcs', type=str, help="func name | FUNC1,...,FUNCn")
    parser.add_argument('--seqs', type=str, help="sequence name | SEQ1,...,SEQn")
    parser.add_argument('--trigs', type=str, help="trigger name | TRG1,...,TRGn")
    parser.add_argument('--views', type=str, help="view name | VW1,...,VWn")
    parser.add_argument('--syns', type=str, help="synonym name | SYN1,...,SYNn")
    parser.add_argument('--idxs', type=str, help="index name | IX1,...,IXn")
    parser.add_argument('--info', action='store_true', help="Show a list of supported objects and exit")

    # this option reeeeeaaaally changes behavior and creates some tedious branching
    # parser.add_argument('--schema', type=str, help="schema containing the objects of interest")

    parser.add_argument('dburl', type=str, help='username/password@tnsalias')
    parser.add_argument(
        'output_path',
        type=str,
        metavar="DIRNAME",
        help="Location to deposit DDL files. Filename will be generated from the schema, object type and object " +
             "name and be placed in DIRNAME."
    )
    args = parser.parse_args()
    return args

#-----------------------------------------
def main(arglist):

    ddlg = DDLGenerator(get_command_line_args())
    ddlg.get_ddl(ddlg.conn, ddlg.schema.upper())
    return 0
#-----------------------------------------

if __name__ == "__main__":
    sys.exit(main(sys.argv))







