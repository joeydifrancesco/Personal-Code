/*************************************************************************************
                                                _             _                 
   __ _   _   _    ___   _   _    ___     ___  | |_    __ _  | |_   _   _   ___ 
  / _` | | | | |  / _ \ | | | |  / _ \   / __| | __|  / _` | | __| | | | | / __|
 | (_| | | |_| | |  __/ | |_| | |  __/   \__ \ | |_  | (_| | | |_  | |_| | \__ \
  \__, |  \__,_|  \___|  \__,_|  \___|   |___/  \__|  \__,_|  \__|  \__,_| |___/  and
     |_|                                                                          
  _       _                               _               _                      _                 
 | |__   (_) __   __   ___    __      __ (_)  _ __     __| |   ___   __      __ (_)  _ __     __ _ 
 | '_ \  | | \ \ / /  / _ \   \ \ /\ / / | | | '_ \   / _` |  / _ \  \ \ /\ / / | | | '_ \   / _` |
 | | | | | |  \ V /  |  __/    \ V  V /  | | | | | | | (_| | | (_) |  \ V  V /  | | | | | | | (_| |
 |_| |_| |_|   \_/    \___|     \_/\_/   |_| |_| |_|  \__,_|  \___/    \_/\_/   |_| |_| |_|  \__, |
                                                                                             |___/ 

*************************************************************************************/


/*************************************************************************************
	You can use the approach shown below to submit your code in the background and 
	then in the same window check the queue status to see how your query is coming 
	along.

	NOTE: 	You need to SAVE your file before you can background submit a section
				of code
			If you background submit and check the queue and do not see anything,
				check the log file in Job history in SAS Environment Manager - 
				there may be an error in your query and the log should show that
			As always, you will need to update the output database to be your own
				and not damod

*************************************************************************************/
%hive_drop_table(schema=damod, table=claims_data);
%LET sql=%STR(CREATE TABLE damod.claims_data AS 
				SELECT header.clm_thru_dt, line.claim_sk
				FROM claims_sample.hosp_header AS header
				LEFT JOIN claims_sample.hosp_line AS line
							ON header.claim_sk = line.claim_sk
				WHERE header.clm_thru_dt > '2019-10-01'
					AND line.clm_thru_dt > '2019-10-01');
%hive_exec_sql(&sql);

* Check the status of the Hive queue;
%queue_status();


/*************************************************************************************
	Hive windowing functions...

	The first query uses Hive windowing functions and the second query uses a DISTINCT. 
	The source for both is the claims_sample database, which contains a small
	subset of claims data. The data in the second query is channeled through 
	a single reducer to sort the data before being able to perform the DISTINCT. 

	The first query looks at the rows of data and groups them by the columns listed
	and simply adds a new column called row_num which has a 1 for the first occurance
	of that key, 2 for the second, etc. By selecting row_num = 1, we only get the first
	row of the data which is effectively the same a performing a DISTINCT.

	With this small of a data set, the second query runs about the same speed as the first
	but both do exactly the	same thing - get a unique list of claims based on the columns 
	requested. As mentioned, this dataset is small so the time difference is minimal.
	When operatingon a years worth of claims data, we have seen queries that take 3 hours
	only take 15 minutes when DISTINCT is replaced by Hive windowing functions.

	Optimizing your code to use Hive windowing functions instead of DISTINCTs will
	significantly reduce the amount of time your query takes to run.

*************************************************************************************/

* Clean up;
%hive_drop_table(schema=damod, table=claims_windowing);
%hive_drop_table(schema=damod, table=claims_distinct);

* Using Hive windowing functions...;
%LET sql=%STR(CREATE TABLE damod.claims_windowing AS
				SELECT *
				FROM
				(
					SELECT header.claim_sk,
						line.rev_cntr_hcpcs_cd,
						header.clm_dgns_cd_1,
						header.clm_dgns_cd_2,
						header.clm_dgns_cd_3,
						header.clm_dgns_cd_4,
						header.clm_dgns_cd_5,
						row_number() OVER (PARTITION BY header.claim_sk, line.rev_cntr_hcpcs_cd) row_num
					FROM claims_sample.hosp_header AS header
					INNER JOIN claims_sample.hosp_line AS line
						ON header.claim_sk = line.claim_sk
				) tbl
				WHERE tbl.row_num = 1);
%hive_exec_sql(&sql);

* Same query, using DISTINCT;
%LET sql=%STR(CREATE TABLE damod.claims_distinct AS
				SELECT DISTINCT
					header.claim_sk,
					line.rev_cntr_hcpcs_cd,
					header.clm_dgns_cd_1,
					header.clm_dgns_cd_2,
					header.clm_dgns_cd_3,a
					header.clm_dgns_cd_4,
					header.clm_dgns_cd_5
				FROM claims_sample.hosp_header AS header
				INNER JOIN claims_sample.hosp_line AS line
					ON header.claim_sk = line.claim_sk);
%hive_exec_sql(&sql);


* Check the queue;
%queue_status();

* Clean up;
%hive_drop_table(schema=damod, table=claims_windowing);
%hive_drop_table(schema=damod, table=claims_distinct);


