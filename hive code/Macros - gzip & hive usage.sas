/*************************************************************************************
                _                                                    
   __ _   ____ (_)  _ __          _   _   ___    __ _    __ _    ___ 
  / _` | |_  / | | | '_ \        | | | | / __|  / _` |  / _` |  / _ \
 | (_| |  / /  | | | |_) |  _    | |_| | \__ \ | (_| | | (_| | |  __/
  \__, | /___| |_| | .__/  ( )    \__,_| |___/  \__,_|  \__, |  \___|    
  |___/            |_|     |/                           |___/        
                                                                                      
*************************************************************************************/

* Define schema/database macro variables;
* NOTE: replace these with your organization's schema/database name;
%LET myschema=your_schema;

* Create LIBREFs;
LIBNAME lib_hive HADOOP &HIVE_LIBNAME SCHEMA=&myschema;		* LIBREF to our project Hive database with CDR defaults;


/*************************************************************************************

	Reminder - the %help() macro will show the syntax for all available macros
                                           
*************************************************************************************/
%help(); 


/*************************************************************************************

	%usage_hive() - This macro will show he tables in your project
						Hive database and the amount of space each table uses 

					This is updated DAILY, not real-time
                                           
*************************************************************************************/
* Show a summary of all project databases (usually just one);
%usage_hive();

* Show a summary for a single project database;
%usage_hive(schema="&myschema");

* Show a summary (table by table) for your project database;
%usage_hive(schema="&myschema", show_tables=Y);


/*************************************************************************************

	Using gzip/gunzip

	This example code will:

		1. Make a copy of this code file
		2. Gzip it - we will confirm the file is compressed
		3. Delete the copy of this code file so we can gunzip the file from step 2
		4. Gunzip the file from Step 2
		5. Clean up - delete the gzip file and the extracted file
                                                                  
*************************************************************************************/
%LET original_file="/workspace/workbench/your_company/data/yourfile.sas";
%LET source_file="/workspace/workbench/your_company/data/yourfile (copy).sas";
%LET gzip_file="/workspace/workbench/your_company/data/yourfile.sas.gz";
%LET output_file=&gzip_file;

* 1. Copy the original file;
%MACRO prep();
	DATA _NULL_;
		FILENAME src &original_file;
		FILENAME des &source_file;
		rc=FCOPY('src', 'des');
		%PUT &rc;
	RUN;
	%sleep(t=2); * Wait to make sure the file is copied;
%MEND;
%prep();


* 2. Gzip the file just created;
%gzip(source_file=&source_file);


* 3. Delete the copied file so we can extract the gzip file since the extracted file will have the same name;
%MACRO delete_file(file_to_delete="invalid file path");
	%LET file_to_delete=%QSYSFUNC(DEQUOTE(&file_to_delete));
	DATA _NULL_;
		LENGTH fname $8;
	    rc=FILENAME(fname, "&file_to_delete");
	    IF rc = 0 and FEXIST(fname) THEN rc=FDELETE(fname);
	    rc=FILENAME(fname);
	RUN;
	%sleep(t=2); * Wait to make sure the file is deleted;
%MEND;
%delete_file(file_to_delete=&source_file);


* 4. Gunzip the file gzipped in step 2;
%gunzip(gzip_file=&gzip_file);


* 5. Clean up - delete the gzip file and extracted file;
%delete_file(file_to_delete=&gzip_file);
%delete_file(file_to_delete=&source_file);
    
