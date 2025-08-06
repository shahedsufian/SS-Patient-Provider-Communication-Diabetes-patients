/*******************************************************************************
*
* SAS Program: End-to-End MEPS Data Import and Cohort Creation
*
*
* Purpose:
* This program performs a complete data processing pipeline for MEPS data.
* It is divided into two main parts:
*
* Part 1: Imports MEPS transport files (.ssp) and converts them into
* permanent SAS datasets (.sas7bdat), organizing them into
* 'panels' and 'medical conditions' libraries.
*
* Part 2: Uses the newly created SAS datasets to build an analytical
* cohort of individuals with diabetes. It identifies the cohort,
* merges panel and medical data, applies inclusion/exclusion
* criteria, and exports the final, cleaned datasets to CSV files
* for use in other statistical software like Python or R.
*
*
*******************************************************************************/


/*******************************************************************************
* PART 1
* IMPORT MEPS TRANSPORT FILES (.ssp)
*******************************************************************************/

* --- 1. SETUP & CONFIGURATION FOR PART 1 --- *;

* Define macro variables for the root directories. This makes the script
* portable and easy to update.
* IMPORTANT: Update these paths to match your local environment.
%let transport_file_path = C:\path\to\your\meps\transport_files;
%let sas_data_path = C:\path\to\your\output\sas_datasets;
%let csv_export_path = C:\path\to\your\output\csv_files;

* Assign libnames for the output SAS datasets. This is where the permanent
* SAS files will be created and stored.
libname panels "&sas_data_path.\Full_Panels";
libname medic_c "&sas_data_path.\Medical_Conditions";


* --- 2. MACRO DEFINITION: %Import_MEPS_File --- *;

* This macro automates the process of importing a single MEPS transport file.
* It dynamically chooses the correct import procedure based on the 'proctype' parameter.
*-------------------------------------------------------------------------------;
%macro Import_MEPS_File(lib=, ds_name=, file_ref=, path=, proctype=);

    * Assign a temporary fileref to the full path of the transport file.
    filename &file_ref. "&transport_file_path.\&path.\&ds_name..ssp";

    * Use a conditional statement to run the appropriate import procedure.
    %if %upcase(&proctype.) = CIMPORT %then %do;
        PROC CIMPORT DATA=&lib..&ds_name. INFILE=&file_ref.;
        RUN;
    %end;
    %else %if %upcase(&proctype.) = XCOPY %then %do;
        PROC XCOPY IN=&file_ref. OUT=&lib. IMPORT;
        RUN;
    %end;

    * Clear the temporary fileref to free up resources.
    filename &file_ref. clear;

%mend Import_MEPS_File;


* --- 3. EXECUTION: Import All Files --- *;

* Call the macro for each file, specifying the correct parameters.

* --- Import Longitudinal Panel Files --- *
%Import_MEPS_File(lib=panels, ds_name=H217, file_ref=p2018_19, path=Full_Panels, proctype=CIMPORT);
%Import_MEPS_File(lib=panels, ds_name=H210, file_ref=p2017_18, path=Full_Panels, proctype=CIMPORT);
%Import_MEPS_File(lib=panels, ds_name=H202, file_ref=p2016_17, path=Full_Panels, proctype=XCOPY);
%Import_MEPS_File(lib=panels, ds_name=H193, file_ref=p2015_16, path=Full_Panels, proctype=XCOPY);

* --- Import Medical Conditions Files --- *
%Import_MEPS_File(lib=medic_c, ds_name=H214, file_ref=m2019, path=Medical_Conditions, proctype=CIMPORT);
%Import_MEPS_File(lib=medic_c, ds_name=H207, file_ref=m2018, path=Medical_Conditions, proctype=CIMPORT);
%Import_MEPS_File(lib=medic_c, ds_name=H199, file_ref=m2017, path=Medical_Conditions, proctype=XCOPY);
%Import_MEPS_File(lib=medic_c, ds_name=H190, file_ref=m2016, path=Medical_Conditions, proctype=XCOPY);
%Import_MEPS_File(lib=medic_c, ds_name=H180, file_ref=m2015, path=Medical_Conditions, proctype=XCOPY);


/*******************************************************************************
* PART 2
* CREATE AND EXPORT DIABETES COHORT
*******************************************************************************/


* --- 4. SETUP & CONFIGURATION FOR PART 2 --- *;

* Define a global macro containing the list of all variables to keep in the final datasets.
* This makes the variable selection step much cleaner and easier to manage.
%global final_vars;
%let final_vars =
    ADLIST2 ADEXPL2 ADRESP2 ADPRTM2 TREATM2 DECIDE2 EXPLOP2 PRVSPK2 RACETHX
    HSPLAP2 WHITPR2 BLCKPR2 ASIANP2 NATAMP2 PACISP2 OTHRCP2 SEX GENDRP2
    OTHLANG LANGSPK HWELLSPE PRVSPK2 AGE1X REGION1 HIDEG POVCATY1 INSCOVY1
    MARRY1X HAVEUS2 PROVTY2 TYPEPE2 LOCATN2 EMPST1 RTHLTH1 MNHLTH1 DUPERSID
    PANEL LONGWT LSAQWT VARSTR VARPSU ADAPPT2 OBDRVY2 RXTOTY2 RXEXPY2
    DSCONF3 ERTOTY2 TOTTCHY2
;


* --- 5. MACRO DEFINITION: %Process_Panel_Year --- *;

* This macro automates the entire data processing pipeline for a single year,
* using the SAS datasets created in Part 1.
*-------------------------------------------------------------------------------;
%macro Process_Panel_Year(year=, med_ds=, panel_ds=);

    /*-- Step 5a: Identify individuals with diabetes for the given year --*/
    data diabetes_&year. (keep = DUPERSID diab_flag year);
        set medic_c.&med_ds.;
        /* Use STARTSWITH for a more robust check of ICD-10 codes */
        where substr(ICD10CDX, 1, 3) in ("E10", "E11");
        diab_flag = 1;
        year = &year.;
    run;

    /*-- Step 5b: Merge diabetes cohort with the corresponding panel data --*/
    proc sort data=diabetes_&year.; by DUPERSID; run;
    proc sort data=panels.&panel_ds.; by DUPERSID; run;

    data diabetes_&year._merged;
        merge diabetes_&year. (in=in_diab)
              panels.&panel_ds. (in=in_panel);
        by DUPERSID;
        /* Keep only individuals present in both datasets (inner join) */
        if in_diab and in_panel;
    run;

    /*-- Step 5c: Apply inclusion/exclusion criteria --*/
    data diabetes_&year._clean;
        set diabetes_&year._merged;
        /* Keep only individuals who were in the panel for the full year and meet
           other criteria (not deceased, institutionalized, military, etc.) */
        where YEARIND=1 and ALL5RDS=1 and DIED=0 and INST=0 and
              MILITARY=0 and ENTRSRVY=0 and LEFTUS=0;
    run;

    /*-- Step 5d: Select final variables and export to CSV --*/
    data final_&year. (keep=&final_vars.);
        set diabetes_&year._clean;
    run;

    /* Export the final, cleaned dataset to a CSV file */
    PROC EXPORT DATA = final_&year.
        DBMS = CSV
        OUTFILE = "&csv_export_path.\d_&year..csv"
        REPLACE;
    RUN;

    /*-- Step 5e: Clean up intermediate work datasets --*/
    proc delete data=diabetes_&year. diabetes_&year._merged diabetes_&year._clean final_&year.;
    run;

%mend Process_Panel_Year;


* --- 6. EXECUTION: Process All Years --- *;

* Call the macro for each year, providing the correct dataset names.
%Process_Panel_Year(year=2019, med_ds=H214, panel_ds=H217);
%Process_Panel_Year(year=2018, med_ds=H207, panel_ds=H210);
%Process_Panel_Year(year=2017, med_ds=H199, panel_ds=H202);
%Process_Panel_Year(year=2016, med_ds=H190, panel_ds=H193);


* --- 7. CLEANUP --- *;
* Clear all libnames at the end of the program.
libname panels clear;
libname medic_c clear;