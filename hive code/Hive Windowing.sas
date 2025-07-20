/*************************************************************************************                                       
  _       _                               _               _                      _                 
 | |__   (_) __   __   ___    __      __ (_)  _ __     __| |   ___   __      __ (_)  _ __     __ _ 
 | '_ \  | | \ \ / /  / _ \   \ \ /\ / / | | | '_ \   / _` |  / _ \  \ \ /\ / / | | | '_ \   / _` |
 | | | | | |  \ V /  |  __/    \ V  V /  | | | | | | | (_| | | (_) |  \ V  V /  | | | | | | | (_| |
 |_| |_| |_|   \_/    \___|     \_/\_/   |_| |_| |_|  \__,_|  \___/    \_/\_/   |_| |_| |_|  \__, |
                                                                                             |___/ 
                                                              
*************************************************************************************/

/*************************************************************************************
	Hive windowing functions...

	The first query we are going to look at performs a DISTINCT on a subset of claims
	data and then joins that with a DISTINCT subset of other claims data. A DISTINCT
	on claims can take hours vs minutes without a DISTINCT. The data in each subset
	is channeled through a single reducer to sort the data before being able to perform 
	a final DISTINCT which is the cause of the severe reduction in processing speed.

	In this scenario, we have a subset that takes hours to produce joining to another
	subset that also takes hours to produce. End result? The cluster usage for this query
	was 90% and it took hours and hours and blocked all other users from running queries
	at the same time.
 
	One other issue... when using a date range for paritions, we recommend using 
		>= and <= rather than BETWEEN - it seems to be clearer for the query optimizer
	
	Executution Plan:
		Reducer 2 <- Map 1 (SIMPLE_EDGE) 								[first subquery]
		Reducer 5 <- Map 1 (SIMPLE_EDGE) 								[second subquery]
		Reducer 3 <- Reducer 2 (SIMPLE_EDGE), Reducer 5 (SIMPLE_EDGE)	[join]
		Reducer 4 <- Reducer 3 (CUSTOM_SIMPLE_EDGE)						[final select]

*************************************************************************************/
%hive_drop_table(damod, pgrivas_ipsn_filter);

%LET sql=%STR(	
		CREATE TABLE damod.pgrivas_ipsn_filter AS 
		SELECT LINE2.CLAIM_SK 
		FROM (SELECT DISTINCT CLAIM_SK 
				FROM NCH_PART_A.IPSN_LINE 
				WHERE REV_CNTR_CD IN ("0760","0761","0762","0769","0450","0451","0452","0456","0459","0981") 
				AND CLM_THRU_DT BETWEEN DATE "2019-06-08" AND DATE "2021-4-30") AS LINE1
		RIGHT JOIN (SELECT DISTINCT CLAIM_SK 
					FROM NCH_PART_A.IPSN_LINE 
					WHERE REV_CNTR_CD NOT IN ("0760","0761","0762","0769","0450","0451","0452","0456","0459","0981") 
					AND CLM_THRU_DT BETWEEN DATE "2019-06-08" AND DATE "2021-4-30") AS LINE2 ON LINE1.CLAIM_SK = LINE2.CLAIM_SK 
		WHERE LINE1.CLAIM_SK IS NULL
);
%hive_explain(&sql);



/*************************************************************************************
	
	This query uses Hive windowing functions. It looks at the rows of data and groups 
	them by the columns listed	and simply adds a new column called row_num which has a 
	1 for the first occurance of that key, 2 for the second, etc. By selecting 
	row_num = 1, we only get the first row of the data which is effectively the same a 
	performing a DISTINCT. This version of the query used up a smaller percentage of the 
	cluster and only took about 8 minutes to run successfully.

	Optimizing your code to use Hive windowing functions instead of DISTINCTs will
	significantly reduce the amount of time your query takes to run.

		Executution Plan:
			Reducer 2 <- Map 1 (SIMPLE_EDGE), Map 5 (SIMPLE_EDGE)	[subselect in parallel]
			Reducer 3 <- Reducer 2 (SIMPLE_EDGE)					[join]
			Reducer 4 <- Reducer 3 (CUSTOM_SIMPLE_EDGE)				[final select]

*************************************************************************************/
*%hive_drop_table(damod, pgrivas_ipsn_filter);

* Updated query;
%LET sql=%STR(
	CREATE TABLE damod.pgrivas_ipsn_filter AS
		SELECT claim_sk
		FROM (
				SELECT claim_sk, ROW_NUMBER() OVER (PARTITION BY claim_sk) AS row_num 
				FROM (

						SELECT LINE2.CLAIM_SK
						FROM (SELECT CLAIM_SK 
								FROM NCH_PART_A.IPSN_LINE 
								WHERE REV_CNTR_CD IN ("0760","0761","0762","0769","0450","0451","0452","0456","0459","0981") 
								AND CLM_THRU_DT >= '2019-06-08' AND CLM_THRU_DT <= '2021-04-30') AS LINE1
						RIGHT JOIN (SELECT CLAIM_SK 
									FROM NCH_PART_A.IPSN_LINE 
									WHERE REV_CNTR_CD NOT IN ("0760","0761","0762","0769","0450","0451","0452","0456","0459","0981") 
									AND CLM_THRU_DT >= '2019-06-08' AND CLM_THRU_DT <= '2021-04-30') AS LINE2 ON LINE1.CLAIM_SK = LINE2.CLAIM_SK 
						WHERE LINE1.CLAIM_SK IS NULL

					) t
				) t2
		WHERE row_num = 1
);
%hive_explain(&sql);


* Extract the SELECT for easier reading;
%LET select_sql=%STR(
						SELECT LINE2.CLAIM_SK
						FROM (SELECT CLAIM_SK 
								FROM NCH_PART_A.IPSN_LINE 
								WHERE REV_CNTR_CD IN ("0760","0761","0762","0769","0450","0451","0452","0456","0459","0981") 
								AND CLM_THRU_DT >= '2019-06-08' AND CLM_THRU_DT <= '2021-04-30') AS LINE1
						RIGHT JOIN (SELECT CLAIM_SK 
									FROM NCH_PART_A.IPSN_LINE 
									WHERE REV_CNTR_CD NOT IN ("0760","0761","0762","0769","0450","0451","0452","0456","0459","0981") 
									AND CLM_THRU_DT >= '2019-06-08' AND CLM_THRU_DT <= '2021-04-30') AS LINE2 ON LINE1.CLAIM_SK = LINE2.CLAIM_SK 
						WHERE LINE1.CLAIM_SK IS NULL
				);


* Wrap the SELECT in a CREATE TABLE and using Windowing functions;
* Note /claim_sk/ is the value we are retrieving as well as the only column in our PARTITION BY group;
%LET sql=%STR(
	CREATE TABLE damod.pgrivas_ipsn_filter AS
		SELECT claim_sk
		FROM (
				SELECT claim_sk, ROW_NUMBER() OVER (PARTITION BY claim_sk) AS row_num 
				FROM (
						&select_sql
					) t
				) t2
		WHERE row_num = 1
);
%hive_explain(&sql);


* Simplify further - at this point the query is very much just a shell that we are passing the details to;
%LET unique_key=claim_sk;
%LET sql=%STR(
	CREATE TABLE damod.pgrivas_ipsn_filter AS
		SELECT &unique_key
		FROM (
				SELECT &unique_key, ROW_NUMBER() OVER (PARTITION BY &unique_key) AS row_num 
				FROM (
						&select_sql
					) t
				) t2
		WHERE row_num = 1
);
%hive_explain(&sql);


* This shell or template can be used for any future queries you need to run that require a DISTINCT;
%LET unique_key=column1, column2, column3;
%LET select_sql=SELECT &unique_key FROM your_table;
%LET sql=%STR(
	CREATE TABLE your_project_database.your_table AS
		SELECT &unique_key
		FROM (
				SELECT &unique_key, ROW_NUMBER() OVER (PARTITION BY &unique_key) AS row_num 
				FROM (
						&select_sql
					) t
				) t2
		WHERE row_num = 1
);
%hive_explain(&sql);

