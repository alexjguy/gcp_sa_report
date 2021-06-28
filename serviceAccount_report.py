from google.cloud import monitoring_v3
import time
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials


project_ids = ["irn-69659-dev-938457410", "irn-69659-int-2058630705", "irn-69659-prd-50"]


credentials = GoogleCredentials.get_application_default()

def replace_sa_id(project_id,id):
    service = discovery.build('iam', 'v1')
    name = f'projects/{project_id}/serviceAccounts/{id}'  # TODO: Update placeholder value.
    request = service.projects().serviceAccounts().get(name=name)
    try:
        response = request.execute()
        return response["email"]
    except HttpError as err:
        if err.resp.status in [403, 500, 503]:
            time.sleep(5)        
        elif err.resp.status in [404]:
            return f'Deleted SA id: {id}'
        else: raise
        
def list_serviceAccounts(project_id):
    service = discovery.build('iam', 'v1')
    name = f'projects/{project_id}'
    request = service.projects().serviceAccounts().list(name=name)
    serviceAccounts = []
    while True:
        response = request.execute()
    
        for service_account in response.get('accounts', []):
            # TODO: Change code below to process each `service_account` resource:
            #print(f'{service_account["email"]} {service_account["uniqueId"]} {get_metric_sa_auth_id(project_id,service_account["uniqueId"])}')
            if "disabled" in service_account.keys():
                if service_account["disabled"] == False:
                    serviceAccounts.append(service_account["uniqueId"])
            else:
                serviceAccounts.append(service_account["uniqueId"])

        request = service.projects().serviceAccounts().list_next(previous_request=request, previous_response=response)
        if request is None:
            break
    return serviceAccounts


def list_serviceAccountsKeys(project_id, serviceAccount):
    service = discovery.build('iam', 'v1')
    name = f'projects/{project_id}/serviceAccounts/{serviceAccount}'
    request = service.projects().serviceAccounts().keys().list(name=name,keyTypes=["USER_MANAGED"])
    serviceAccountsKeys = []
    response = request.execute()

    for service_account in response.get('keys', []):
        serviceAccountsKey = {}
        serviceAccountsKey["name"] = service_account["name"].split("/")[5]
        serviceAccountsKey["validAfterTime"] = service_account["validAfterTime"]
        serviceAccountsKey["validBeforeTime"] = service_account["validBeforeTime"]
        serviceAccountsKeys.append(serviceAccountsKey)

    return serviceAccountsKeys

def get_metric_sa_key_auth_id(project_id):
    client = monitoring_v3.MetricServiceClient()

    project_name = f"projects/{project_id}"
    interval = monitoring_v3.TimeInterval()

    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": (seconds - 2592000), "nanos": nanos},
        }
    )
    aggregation = monitoring_v3.Aggregation(
        {
            "alignment_period": {"seconds": 2592000},  # 20 minutes
            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
            "group_by_fields": ["resource.label.unique_id","metric.label.key_id"],

        }
    )
    results = client.list_time_series(
        request={
            "name": project_name,
            "filter": f'metric.type = "iam.googleapis.com/service_account/key/authn_events_count" AND resource.type:"iam_service_account"',
            "interval": interval,
            "aggregation": aggregation,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )
    saKeysMetricResults = {}
    for result in results:
        # print(result)
        # print(result.metric.labels["key_id"])
        # print(result.points[0].value.int64_value)
        saKeysMetricResults[result.metric.labels["key_id"]] = result.points[0].value.int64_value
        
    return(saKeysMetricResults)


def get_metric_sa_auth_id(project_id):
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"
    interval = monitoring_v3.TimeInterval()

    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": (seconds - 2592000), "nanos": nanos},
        }
    )
    aggregation = monitoring_v3.Aggregation(
        {
            "alignment_period": {"seconds": 2592000},  # 20 minutes
            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            #"cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_SUM,
            "group_by_fields": ["resource.label.unique_id"],

        }
    )
    results = client.list_time_series(
        request={
            "name": project_name,
            "filter": f'metric.type = "iam.googleapis.com/service_account/authn_events_count" AND resource.type:"iam_service_account"',
            "interval": interval,
            "aggregation": aggregation,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )
    saMetricResults = {}
    
    for result in results:
        saMetricResults[result.resource.labels["unique_id"]] = result.points[0].value.int64_value
    return(saMetricResults)

if __name__ == "__main__":
    # now = time.time()
    # seconds = int(now)
    # nanos = int((now - seconds) * 10**9)
    # timestamp = Timestamp(seconds=seconds, nanos=nanos)
    print("Project ID,Service Account,Service Account ID,Service Account Key ID,Create Time,Expire Time,Authorizations in the last month,Total")
    for project_id in project_ids:
        #print(f'Project ID: {project_id}')
        
        serviceAccounts = list_serviceAccounts(project_id)
        saMetricResults = get_metric_sa_auth_id(project_id)
        saKeysMetricResults = get_metric_sa_key_auth_id(project_id)
        saNoAuth = 0
        saKeyNoAuth = 0
        for serviceAccount in serviceAccounts:
            if serviceAccount in saMetricResults:
                #print(f'  Service Account: {replace_sa_id(project_id,serviceAccount)}, Service Account Id: {serviceAccount}, Authorizations in the last month: {saMetricResults[serviceAccount]}')
                print(f'{project_id},{replace_sa_id(project_id,serviceAccount)},{serviceAccount},,,,{saMetricResults[serviceAccount]}')
            else:
                #print(f'  Service Account: {replace_sa_id(project_id,serviceAccount)}, Service Account Id: {serviceAccount}, Authorizations in the last month: {None}')
                print(f'{project_id},{replace_sa_id(project_id,serviceAccount)},{serviceAccount},,,,{None}')
                saNoAuth += 1
            serviceAccountsKeys = list_serviceAccountsKeys(project_id, serviceAccount)
            for serviceAccountsKey in serviceAccountsKeys:
                if serviceAccountsKey["name"] in saKeysMetricResults:
                    #print(f'    SA Key Id: {serviceAccountsKey["name"]}, created on {serviceAccountsKey["validAfterTime"]}, valid until: {serviceAccountsKey["validBeforeTime"]}, Authorizations in the last month: {saKeysMetricResults[serviceAccountsKey["name"]]}')
                    print(f'{project_id},{replace_sa_id(project_id,serviceAccount)},{serviceAccount},{serviceAccountsKey["name"]},{serviceAccountsKey["validAfterTime"]},{serviceAccountsKey["validBeforeTime"]},{saKeysMetricResults[serviceAccountsKey["name"]]}')
                else:
                    #print(f'    SA Key Id: {serviceAccountsKey["name"]}, created on {serviceAccountsKey["validAfterTime"]}, valid until: {serviceAccountsKey["validBeforeTime"]}, Authorizations in the last month: {None}')
                    print(f'{project_id},{replace_sa_id(project_id,serviceAccount)},{serviceAccount},{serviceAccountsKey["name"]},{serviceAccountsKey["validAfterTime"]},{serviceAccountsKey["validBeforeTime"]},{None}')
                    saKeyNoAuth += 1
        print(f'{project_id},,,,,,,Number of SAs not used: {saNoAuth}')
        print(f'{project_id},,,,,,,Number of SA Keys not used: {saKeyNoAuth}')
