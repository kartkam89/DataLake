# DataLake

Here are the list of python files used for datalake project:

1. ar_datalakev5.py - Reads VoT file, preprocess Tier 1 records, writes data to ACS_RGWW1.AR_VOT_TIER1
2. ar_datalakee_t2.py - Reads VoT file, preprocess Tier 2 records, writes data to ACS_RGWW1.AR_VOT_TIER2
3. ar_datalake_vlrc.py - Reads revenue data from ADR, writes data to ACS_RGWW1.AR_REVENUE_SUMMARY
4. downloading_file.py - Loads asset (Webpage/pdf) and saves them as pdf
5. extracting_compsv1.py - Reads text from pdf, and classifies asset to a firm
6. html_pdf.py - Converts html pages to pdf
7. pi_key_generator.py - Creates Unique Key for Tier 2 reports and AR Pros
8. predict_firm.py - Takes text from first two pages of asset, Vectorises, Predict Firm using pretrained model
9. querying.py - Takes ODBC DSN as parameter and establishes connection with Database
10. report_pipeline.py - Generates a report with each AR report as row and published versions as columns 
11. standardiser.py - Maps custom asset names to standard asset name
12. text_best_match.py - Matches string to best possible match from list of string
13. writing_tablev1.py - Takes Dataframe as input, defines data type for each column and pushes to Database 
