# Patients in ED

## Context
This repo ingests Unified SUS ECDS data and outputs data showing the level of A&E visitors and admissions per hour. By default, this code will output to [Data_Lab_NCL_Dev].[JakeK].[uec_patientsined].

## Usage
### First Time Setup
Please refer to the Scripting onboard document ([here](https://nhs-my.sharepoint.com/:w:/r/personal/emily_baldwin20_nhs_net/Documents/Documents/Infrastructure/Skills%20Development/Onboarding%20resources/Scripting_Onboarding.docx?d=w7ff7aa3bbbea4dab90a85f1dd5e468ee&csf=1&web=1&e=DTsN3A)) for instructions on installing python and setting up a virtual environment for the project. If the file is unavailable or you have queries, please contact the Head of Data Science for guidance.

You will also need the .env file not included in the public repo. This can be found in the UEC portfolio directory: 
```
\UEC and Productivity Team\UEC\patients_in_ed
```

### Execution
The process can be carried out by executing the patients_in_ed.py script in the repo. The default settings are pre-configured but can be edited in the env file.

## Output
This process outputs the processed data directly to the data warehouse. The defitions for the key metrics are:
* count_patients: This is a snapshot of the number of patients in the A&E site at the start of the hour (i.e. 13:00:00 for 1pm).
* count_admissions: This is the number of admissions that occur within every 1 hour window (i.e. admissions that occur between 13:00:00 and 13:59:59 will be considered an admission for 1pm).