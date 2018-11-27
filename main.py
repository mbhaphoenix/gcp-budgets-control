import logging
import os
from google.cloud import firestore

__db = firestore.Client()
# I prefixed the doc name with 0- to be shown first in Google Cloud Console in Cloud Firestore Data screen
__costs_per_interval_starts_doc_name = u'0-costs-per-interval-starts'


def handle_budgets_notifications(data, context):
    """
    Background Cloud Function to be triggered by Pub/Sub when a budget notification is sent to the topic

    :param data (dict): The dictionary with data specific to the budget API `notification <https://cloud.google.com/billing/docs/how-to/budgets#notification_format>`_
    :param context (google.cloud.functions.Context): The Cloud Functions event metadata.
    """

    import base64
    import json
    pubsub_budget_notification_data = json.loads(base64.b64decode(data['data']).decode('utf-8'))

    # The budget alert name must be created with the project_id you want to cap
    budget_project_id = pubsub_budget_notification_data['budgetDisplayName']

    # The budget amount to cap costs
    budget = pubsub_budget_notification_data['budgetAmount']

    logging.info('Handling budget notification for project id: {}'.format(budget_project_id))
    logging.info('The budget is set to {}'.format(budget))
    logging.info('Handling the cost amount of : {} for the budget notification period / month, or technically '
                 'the costIntervalStart : {}'.format(pubsub_budget_notification_data['costAmount'],
                                                     pubsub_budget_notification_data['costIntervalStart']))



    # Each project id will have a collection in Cloud Firestore.
    # The collection name will be suffixed with the project id to cap
    collection_name = __get_collection_name(budget_project_id)

    # If the billing is already disabled, stop Cloud Function execution
    if not __is_billing_enabled(budget_project_id):
        raise RuntimeError('Billing already in disabled state')
    else:
        # get total costs accrued per budget alert period
        costs_per_interval_starts_dict = __get_costs_per_interval_starts_dict(collection_name, pubsub_budget_notification_data)
        logging.info('costs per interval starts items : {}'.format(costs_per_interval_starts_dict))

        # persist the costs per interval and the budget notification in Cloud Firestore as batch
        __persist_data(collection_name, costs_per_interval_starts_dict, pubsub_budget_notification_data)

        # Calculating the total as the sum of the cost amounts which are values to start intervals keys in the dict
        total = sum(costs_per_interval_starts_dict.values())
        logging.info('Total cost amount is {}'.format(total))



        # Disable or not the billing for the project id depending on the total and the budget
        __handle_billing_depending_on_total(budget_project_id, budget, total)


def __get_cloud_billing_service():
    """
    At runtime, Cloud Functions uses the service account PROJECT_ID@appspot.gserviceaccount.com,
    which has the Editor role on the project.

    This function will handle the authentication and the authorization for Cloud Billing API Ressource object
    to interact with the API

    :return:
        A Cloud Billing Resource object with methods for interacting with `Cloud Billing API <https://developers.google.com/resources/api-libraries/documentation/cloudbilling/v1/python/latest/>`_
    """

    # Creating credentials to be used for authentication, by using the Application Default Credentials
    # for the Cloud Function runtime environment
    # The credentials are created  for cloud-billing scope
    from oauth2client.client import GoogleCredentials
    credentials = GoogleCredentials.get_application_default()

    # Using Python Google API Client Library to construct a Resource object for interacting with an Cloud Billing API
    # The name and the version of the API to use can be found here https://developers.google.com/api-client-library/python/apis/
    from apiclient import discovery
    return discovery.build('cloudbilling', 'v1', credentials=credentials, cache_discovery=False)


def __is_billing_enabled(project_id):
    """
    Check if the billing is enabled for a given project_id
    :param project_id: project_id to cap costs for and check billing status
    :return: whether the billing is enabled for the given project_id
    """

    service = __get_cloud_billing_service()

    # https://developers.google.com/resources/api-libraries/documentation/cloudbilling/v1/python/latest/cloudbilling_v1.projects.html#getBillingInfo
    billing_info = service.projects().getBillingInfo(name='projects/{}'.format(project_id)).execute()
    if not billing_info or 'billingEnabled' not in billing_info:
        return False
    return billing_info['billingEnabled']


def __get_costs_per_interval_starts_dict(collection_name, budget_notification_data):
    """
    Construct and get total costs accrued per budget alert period
    :param collection_name: Cloud Firestore collection name where we will store our data
    :param budget_notification_data: the data section of the `budget notification format <https://cloud.google.com/billing/docs/how-to/budgets#notification_format>`_
    :return: The dict where keys are costIntervalStart value (start of the budget alert period)
    and the dict values are costAmount accrued the budget alert period
    """

    from datetime import datetime
    budget_notification_data['addedAt'] = datetime.now().isoformat()
    costs_per_interval_starts_dict = __db.collection(collection_name).document(__costs_per_interval_starts_doc_name).get().to_dict()
    if costs_per_interval_starts_dict:
        costs_per_interval_starts_dict[budget_notification_data['costIntervalStart']] = \
            budget_notification_data['costAmount']
    else:
        costs_per_interval_starts_dict = \
            {budget_notification_data['costIntervalStart']: budget_notification_data['costAmount']}
    return costs_per_interval_starts_dict


def __disable_billing_for_project(project_id):
    """
    Check if the billing is enabled for a given project_id
    :param project_id: project_id to cap costs for, by disabling billing
    :return:
    """

    service = __get_cloud_billing_service()
    # https://developers.google.com/resources/api-libraries/documentation/cloudbilling/v1/python/latest/cloudbilling_v1.projects.html#updateBillingInfo
    billing_info = service.projects()\
        .updateBillingInfo(name='projects/{}'.format(project_id), body={'billingAccountName': ''}).execute()
    assert 'billingAccountName' not in billing_info


def __persist_data(collection_name, costs_per_interval_starts_dict, budget_notification_data):
    """
    Use a batch to :
        - update the costs_per_interval_starts document with the new cost amount value for the period
        - add the budget notification document to the collection to keep track of the different budget notifications

    :param project_id: project_id to cap costs for, by disabling billing
    :return:
    """

    batch = __db.batch()
    costs_per_interval_starts_ref = __db.collection(collection_name).document(__costs_per_interval_starts_doc_name)
    batch.set(costs_per_interval_starts_ref, costs_per_interval_starts_dict)
    budget_notification_data_collection_ref = __db.collection(collection_name).document()
    batch.set(budget_notification_data_collection_ref, budget_notification_data)
    batch.commit()


def __get_collection_name(budget_project_id):
    """
    In Cloud Firestore we will have a collection per project_id to cap costs for.
    This function will Build a collection name for Cloud Firestore with a prefix gotten from Cloud Function env var
    and the project_id to cap costs for
    :param budget_project_id: project_id to cap costs for
    :return: Cloud Firestore collection name for the project_id
    """

    collection_name_prefix = os.environ.get('COLLECTION_NAME_PREFIX', 'budget-notifications')
    return collection_name_prefix + '-' + budget_project_id


def __handle_billing_depending_on_total(budget_project_id, budget, total):
    """
    The logic about disabling or not the billing for the project_id depending on the costs total accrued.
    If the total is greater than the budget we disable the billing for the project_id
    This is how we cap costs :)
    :param budget_project_id: the project_id to cap
    :param budget: the fixed budget
    :param total: the costs total to compare to the budget
    :return:
    """

    if total < budget:
        logging.info('No action shall be taken on total cost amount of {}'.format(total))
    else:
        logging.info('Total cost amount is more than {} euros and disabling billing for project id {}'
                     .format(budget, budget_project_id))
        __disable_billing_for_project(budget_project_id)
