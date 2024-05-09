from dotenv import load_dotenv
import os
import requests
import json
from datetime import datetime

load_dotenv()

token = os.getenv('ACCESS_TOKEN')

all_contacts = []
leads_ids_to_delete = []

def get_contacts(url,headers):
    try:
        response = requests.request("GET", url, headers=headers).json()
        if 'paging' in response:
            after = response['paging']['next']['after']
        else:
            after = False
        contacts = response['results']
        all_contacts.extend(contacts)
    except requests.exceptions.RequestException as e:
        print(f"A Requests error occurred: {e}")
        return {"after": False}
    except Exception as e:
        print(f"An error occurred: {e}")
        return {"after": False}
    return {"after": after}

associations = "leads"

def parse_date(date_string):
    try:
        return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%SZ')

def identify_leads_to_delete(leads,headers):
    url = "https://api.hubapi.com/crm/v3/objects/leads/batch/read?archived=false"

    payload = json.dumps({
    "inputs": leads,
    "properties": [
        "hs_pipeline_stage"
    ]
    })
    headers['Content-Type'] = 'application/json'
    try:
        response = requests.request("POST", url, headers=headers, data=payload).json()
        associated_leads = response['results']
        duplicated_leads = sorted(associated_leads, key=lambda x: parse_date(x['properties']['hs_createdate']))
        duplicated_leads_ids = [item['id'] for item in duplicated_leads[1:]]
        leads_ids_to_delete.extend(duplicated_leads_ids)
    except requests.exceptions.RequestException as e:
        print(f"A Requests error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def delete_duplicated_leads(inputs):

    url = "https://api.hubapi.com//crm/v3/objects/leads/batch/archive"

    payload = json.dumps({
    "inputs": inputs
    })
    headers['Content-Type'] = 'application/json'
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
    except requests.exceptions.RequestException as e:
        print(f"A Requests error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    print(response.text)


url = f"https://api.hubapi.com/crm/v3/objects/contacts?limit=100&associations={associations}&paginateAssociations=false&archived=false"

headers = {
  'Authorization': f'Bearer {token}'
}

after = True
print("Fetching all contacts...")
while after:
    result = get_contacts(url,headers)
    after = result.get('after', False)
    if after:
        url = f"https://api.hubapi.com/crm/v3/objects/contacts?limit=100&associations={associations}&paginateAssociations=false&archived=false&after={after}"
print("Fetching duplicated leads...")
contacts_with_duplicated_leads = [[{'id': lead_id} for lead_id in set(lead['id'] for lead in item['associations']['leads']['results'])] for item in all_contacts if 'associations' in item and 'leads' in item['associations'] and len(set(lead['id'] for lead in item['associations']['leads']['results'])) > 1]

for leads in contacts_with_duplicated_leads:
    identify_leads_to_delete(leads,headers)
print("Number of leads to delete:")
print(len(leads_ids_to_delete))
leads_ids_to_delete_inputs = [{'id': item} for item in leads_ids_to_delete]
delete_duplicated_leads(leads_ids_to_delete_inputs)