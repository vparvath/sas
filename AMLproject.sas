libname Varun "C:\Users\varun\Desktop\fall\data\sas lab";




Data aml;
Set Varun.Synthetic;

run;


data aml1;
set aml;
where icd9_dgns_cd_1 in: ('205');
run;


data aml2;
set aml;
where icd9_dgns_cd_1 in: ('205')
or icd9_dgns_cd_2 in: ('205')
or icd9_dgns_cd_3 in: ('205')
or icd9_dgns_cd_4 in: ('205')
or icd9_dgns_cd_5 in: ('205')
or icd9_dgns_cd_6 in: ('205')
or icd9_dgns_cd_7 in: ('205')
or icd9_dgns_cd_8 in: ('205')
or icd9_dgns_cd_9 in: ('205')
or icd9_dgns_cd_10 in: ('205');
run;


Data A1; Set aml2;
OBS= 1;
Run;


Proc SQL;
Create table PATIENT as 
Select DESYNPUF_ID, 
MIN (CLM_THRU_DT) AS INDEXDT, 
sum (obs) as obs
From A1 
Group by DESYNPUF_ID; 
Quit;



Proc SQL;
Create table PATIENTallclaims as 
Select * from Patient
Left join aml
On PATIENT.DESYNPUF_ID=aml.DESYNPUF_ID;
Quit;


Data PATIENTALLCLAIMSPRE;
Set PATIENTallclaims;
Where CLM_THRU_DT < INDEXDT;
Run;


Data PATIENTALLCLAIMSPOST;
Set PATIENTallclaims;
Where CLM_THRU_DT >= INDEXDT;
Run;


proc print data= PATIENTALLCLAIMSPRE;
run;

Data trans;
set aml2;
where icd9_prcdr_cd_1 in: ('38240')
or icd9_prcdr_cd_2 in: ('38240')
or icd9_prcdr_cd_3 in: ('38240')
or icd9_prcdr_cd_4 in: ('38240')
or icd9_prcdr_cd_5 in: ('38240')
or icd9_prcdr_cd_6 in: ('38240');
run;

data trans;
set aml2;
trans=0; 
array a (6) icd9_prcdr_cd_1-icd9_prcdr_cd_6;
do i=1 to 6; 
if a (i) in : ('38240') then trans=1;  
end; drop i; run; 


/**** op in *****/

data outpatient;
set tmp1.Outpatient_08to10;
run;





Proc SQL;
Create table amlop as 
Select * from Patient
Left join outpatient
On PATIENT.DESYNPUF_ID=outpatient.DESYNPUF_ID;
Quit;


Data amlopv2;
set amlop;
where CLM_THRU_DT ^= .;
keep;
run;

Data amplopv3;
set amlopv2;
D1= input( put( CLM_THRU_DT, YYMMDD8.), YYMMDD10.);
run;


Data amlopv4;
set amplopv3;
new_index= input( put( INDEXDT, 8.), YYMMDD10.);
run;


Data amlop_post;
set amlopv4;
Where D1 >= new_index;
Run;


/**********/



Proc SQL;
Create table amlopcosttotal as 
Select DESYNPUF_ID, sum(clm_pmt_amt) as opcosts from amlop_post
Group by DESYNPUF_ID;
Quit;


/*** carrier file in *****/

data carrier;
set tmp1.Carrier_08to10;
run;


Proc SQL;
Create table amlcarrier as 
Select * from Patient
Left join carrier
On PATIENT.DESYNPUF_ID=carrier.DESYNPUF_ID;
Quit;



Data carrierv2;
set amlcarrier;
where CLM_THRU_DT ^= .;
keep;
run;

Data carrierv3;
set carrierv2;
D1= input( put( CLM_THRU_DT, YYMMDD8.), YYMMDD10.);
new_index= input( put( INDEXDT, 8.), YYMMDD10.);
run;


Data carrier_post;
set carrierv3;
Where D1 >= new_index;
Run;




Data carriercosts;
set carrier_post;
TOTlin = LINE_NCH_PMT_AMT_1
+ LINE_NCH_PMT_AMT_2
+ LINE_NCH_PMT_AMT_3
+ LINE_NCH_PMT_AMT_4
+ LINE_NCH_PMT_AMT_5
+ LINE_NCH_PMT_AMT_6
+ LINE_NCH_PMT_AMT_7
+ LINE_NCH_PMT_AMT_8
+ LINE_NCH_PMT_AMT_9
+ LINE_NCH_PMT_AMT_10
+ LINE_NCH_PMT_AMT_11
+ LINE_NCH_PMT_AMT_12
+ LINE_NCH_PMT_AMT_13;
run;



Proc SQL;
Create table pt_amlctotcost as 
Select DESYNPUF_ID, sum(TOTlin) as totcarriercosts from carriercosts
Group by DESYNPUF_ID;
Quit;


/**inpatient costs***/

Proc SQL;
Create table pt_amlitotcost as 
Select DESYNPUF_ID, sum(clm_pmt_amt) as totipcosts from Patientallclaimspost
Group by DESYNPUF_ID;
Quit;


/***merging****/

Data final_analytic;
merge patient Pt_amlitotcost  Amlopcosttotal Pt_amlctotcost;
run;


/*** total costs ******/
data total_analytic;
set final_analytic;
if opcosts=. then opcosts=0;
if totcarriercosts=. then totcarriercosts=0;
Totalcosts = totipcosts
+totcarriercosts
+opcosts;
run;


proc print data= Total_analytic;
run;

proc means data= Total_analytic;
var Totalcosts;
run;

/**** end of part-1 *****/

/*
libname sasfile 'C:\Users\varun\Desktop\npi data';
libname csvfile XPORT 'C:\Users\varun\Desktop\npi data\NPPES_Data_Dissemination_November_2017\npidata_20050523-20171112.csv' access= readonly;
proc copy inlib= csvfile outlib= sasfile;
run;

*/

PROC IMPORT OUT= WORK.A5 
            DATAFILE= "C:\Users\varun\Desktop\npi data\NPPES_Data_Dissem
ination_November_2017\npidata_20050523-20171112.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

/*****calculating los*******/
Proc SQL;
Create table los as 
Select DESYNPUF_ID, sum(CLM_UTLZTN_DAY_CNT) as totlos from Patientallclaimspost
Group by DESYNPUF_ID;
Quit;


Proc SQL;
Create table post_hospitalisation as 
Select DESYNPUF_ID, sum(obs) as hospitalisation from Patientallclaimspost
Group by DESYNPUF_ID;
Quit;



Data losperhosp;
merge post_hospitalisation los ;
run;


/****clubbing los and final amt******/
Data Final_amtlos;
merge Total_analytic los post_hospitalisation;
run;






/******working on partD*********/

data partD;
set tmp1.Part_d_2008_2011;
run;


data mainfile;
set tmp5.Analysisfinal;
run;

Proc SQL;
Create table patient_partd as 
Select * from mainfile
Left join partD
On mainfile.DESYNPUF_ID=partD.DESYNPUF_ID;
Quit;





data D_datechange;
set patient_partd;
new_date= input( put( SRVC_DT, YYMMDD8.), YYMMDD10.);
new_index= input( put( INDEXDT, 8.), YYMMDD10.);
RUN;

Data  D_datechange_Final;
set  D_datechange;
where SRVC_DT ^= .;
keep;
run;


Data Partd_pt_indexing;
set D_datechange_Final;
Where new_date >= new_index;
Run;

/*******redbook in******/

Data Rbook;
set tmp2.Redbook;
run;

/********MERGING*************/

Proc SQL;
Create table rbook_merge as 
Select * from Partd_pt_indexing
Left join Rbook
On Partd_pt_indexing.PROD_SRVC_ID=Rbook.NDCNUM;
Quit;

/********REMOVE NULLS*************/

Data rm_final;
set rbook_merge;
where PKSIZE ^= .;
keep;
run;

/********DRUG CLASS COSTS*************/
Proc SQL;
Create table Drug_class_cost as 
Select THRCLDS, sum(obs) as claims, sum(TOT_RX_CST_AMT) as total_cost from rm_final
Group by THRCLDS;
Quit;

proc sort data=Drug_class_cost out= sort_Drug_class;
by total_cost;
run;


/********DRUG GROUP COSTS*************/

Proc SQL;
Create table Drug_GROUP_cost as 
Select THRGRDS,THRCLDS, sum(obs) as claims, sum(TOT_RX_CST_AMT) as total_cost from rm_final
Group by THRGRDS;
Quit;

proc sort data=Drug_GROUP_cost out=sort_Drug_GROUP;
by total_cost;
run;

/**Removing nulls**/

proc Sql;
Create table nonull_groups as
select * from sort_Drug_GROUP where THRGRDS is Not null;
Quit;


proc Sql;
Create table nonull_class as
select * from sort_Drug_class where THRCLDS is Not null;
Quit;

PROC EXPORT DATA= WORK.NONULL_CLASS 
            OUTFILE= "C:\Users\varun\Desktop\npi data\class.csv" 
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;


PROC EXPORT DATA= WORK.NONULL_GROUPS 
            OUTFILE= "C:\Users\varun\Desktop\npi data\group.csv" 
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;


proc TTEST data= NONULL_GROUPS;
var total_cost;
run;



/****enrollment in****/

data enrol08;
set tmp3.Enrol_2008;
run;


data enrol09;
set tmp3.Enrol_2009;
run;


data enrol10;
set tmp3.Enrol_2010;
run;


data a;
set enrol08;
run;




proc append base=a data=enrol10;
run;


Proc SQL;
Create table PATIENT_a as 
Select * from Patient
Left join a
On Patient.DESYNPUF_ID=a.DESYNPUF_ID;
Group by DESYNPUF_ID;
Quit;



Data PATIENT_a2;
set PATIENT_a;
where BENE_BIRTH_DT ^= .;
keep;
run;

data PATIENT_a2_datechange;
set PATIENT_a2;
new_dob= input( put(BENE_BIRTH_DT, YYMMDD8.), YYMMDD10.);
new_index= input( put( INDEXDT, 8.), YYMMDD10.);
year= year(BENE_BIRTH_DT);
index= substr(left(INDEXDT),1,4);
RUN;

data age;
set PATIENT_a2_datechange;
age= index-year;
run;

proc sql;
create table agev2 as 
select desynpuf_id



Proc SQL;
Create table Drugs as 
Select DISTINCT THRGRDS, THRCLDS, sum(TOT_RX_CST_AMT) as total_cost from rm_final
Group by THRGRDS;
Quit;



data project;
set tmp5.Analysisfinal;
run;


proc means data= project;
var partDcost;
run;

/*****ttest*********/
proc sql;
create table chemotherapy as 
select * from project where chemo>0;
quit;


proc sql;
create table nochemotherapy as 
select * from project where chemo=0;
quit;

proc freq data= chemotherapy;
tables inpatientcost;
run;

proc means data= nochemotherapy;
var inpatientcost;
run;

data project1;
set project;
where chemo <2;
keep;
run;






proc TTEST data= project1;
class chemo;
var totalcost;
run;
