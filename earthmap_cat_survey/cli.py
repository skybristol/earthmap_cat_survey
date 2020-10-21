import sys, getopt
import earthmap_cat_survey
import pandas as pd
import pickle
from collections import Counter
import datetime
import os
import pylinkedcmd

try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

from . import data

cmd_isaid = pylinkedcmd.pylinkedcmd.Isaid()

def main(argv):
    survey_source_file = None
    isaid_cache_file = None
    output_file_name = f"augmented_survey_data_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx"

    try:
        opts, args = getopt.getopt(argv,"hs:c:o:",["survey=","cache=","output="])
    except getopt.GetoptError:
        print ('error in arguments, use: earthmap_cat_survey -s <inputfile> -c <isaid cache> -o <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('earthmap_cat_survey -s <inputfile> -c <isaid cache> -o <outputfile>')
            sys.exit()
        elif opt in ("-s", "--survey"):
            survey_source_file = arg
        elif opt in ("-c", "--cache"):
            isaid_cache_file = arg
        elif opt in ("-o", "--output"):
            output_file_name = arg
    
    if not os.path.exists(survey_source_file):
        raise Exception("You must supply a path to the survey source file")

    if not os.path.exists(isaid_cache_file):
        raise Exception("You must supply a path to the initial cache of iSAID data")

    data_model_sheet = pd.read_excel(pkg_resources.open_binary(data, 'EMCapacitySurveyDataModel.xlsx'))
    data_model_flat = data_model_sheet.where(pd.notnull(data_model_sheet), None).to_dict(orient="records")

    responses = pd.read_excel(
        survey_source_file,
        usecols = [i["col_index"] for i in data_model_flat if i["property"] is not None],
        names = [i["property"] for i in data_model_flat if i["property"] is not None]
    )
    responses = responses.sort_values('start_time').drop_duplicates('email', keep='first').sort_index()
    responses = responses.where(pd.notnull(responses), None)
    source_emails = responses.email.to_list()

    isaid_data = pickle.load(open(isaid_cache_file, "rb"))
    existing_emails = [i["identifier_email"] for i in isaid_data["directory"]]

    process_emails = [i for i in source_emails if i not in existing_emails]

    if len(process_emails) > 0:
        new_isaid_data = cmd_isaid.assemble_person_record(process_emails, datatypes=["directory","assets","claims"])

        isaid_data["directory"].extend(new_isaid_data["directory"])
        isaid_data["assets"].extend(new_isaid_data["assets"])
        isaid_data["claims"].extend(new_isaid_data["claims"])

        pickle.dump(isaid_data, open(isaid_cache_file, "wb"))

        print("Processed new emails: ", process_emails)
        print("Saved new cache file: ", isaid_cache_file)

    isaid_summary = list()
    for entity in isaid_data["directory"]:
        entity_record = {
            "identifier_email": entity["identifier_email"],
            "displayname": entity["displayname"],
            "jobtitle": entity["jobtitle"],
            "organization_name": entity["organization_name"],
            "organization_uri": entity["organization_uri"],
            "url": entity["url"]
        }

        entity_assets = [i for i in isaid_data["assets"] if i["identifier_email"] == entity["identifier_email"]]
        if len(entity_assets) > 0:
            entity_record["scientific_assets_summary"] = Counter([i["additionaltype"] for i in entity_assets]).most_common()
            entity_record["first_year_published"] = min([i["datepublished"] for i in entity_assets])
            entity_record["last_year_published"] = max([i["datepublished"] for i in entity_assets])

        entity_claims = [
            i for i in isaid_data["claims"] 
            if i["subject_identifier_email"] == entity["identifier_email"]
        ]
        if len(entity_claims) > 0:
            entity_record["job_titles"] = list(set([i["object_label"] for i in entity_claims if i["property_label"] == "job title"]))
            entity_record["organization_affiliations"] = list(set([i["object_label"] for i in entity_claims if i["property_label"] == "organization affiliation"]))
            entity_record["distinct_coauthors"] = len(list(set([i["object_label"] for i in entity_claims if i["property_label"] == "coauthor"])))
            entity_record["expertise_terms"] = list(set([i["object_label"] for i in entity_claims if i["property_label"] == "expertise"]))
            entity_record["metadata_keywords"] = list(set([i["object_label"] for i in entity_claims if i["property_label"] == "keyword"]))

        isaid_summary.append(entity_record)

    df_isaid_summary = pd.DataFrame(isaid_summary)

    enhanced_survey_results = pd.merge(
        left=responses, 
        right=df_isaid_summary, 
        how='left', 
        left_on='email', 
        right_on='identifier_email'
    )

    enhanced_survey_results = enhanced_survey_results.where(
        pd.notnull(enhanced_survey_results), 
        None
    ).to_excel(
        output_file_name,
        index=False
    )

    print("Saved augmented results to file: ", output_file_name)

if __name__ == '__main__':
    main(sys.argv[1:])
