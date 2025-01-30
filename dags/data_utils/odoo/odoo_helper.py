import xmlrpc.client
from datetime import datetime
from airflow.models import Variable
import requests
from airflow.hooks.base import BaseHook
import pandas as pd


def fetch_invoices(models, db, uid, api_key):
    """
    Fetch invoices with the required fields and conditions.
    """
    # Search for invoices
    invoice_ids = models.execute_kw(
        db, uid, api_key,
        'account.move', 'search',
        [[
            ['move_type', 'in', ['out_invoice', 'out_refund']]
        ]]
    )

    # Read invoice details
    invoices = models.execute_kw(
        db, uid, api_key,
        'account.move', 'read',
        [invoice_ids],
        {'fields': [
            'id', 'name', 'move_type', 'partner_id', 'invoice_date',
            'invoice_date_due', 'create_date', 'line_ids',
            'status_in_payment', 'x_studio_csv_gnr'
        ]}
    )

    # Filter invoices to keep only those with specific conditions
    invoices_to_keep = [
        invoice for invoice in invoices
        if invoice['status_in_payment'] not in ['draft', 'blocked', 'cancel'] and invoice['x_studio_csv_gnr'] == False
    ]

    return invoices_to_keep


def fetch_lines(models, invoice, db, uid, api_key):
    """
    Fetch accounting lines for a given invoice.
    """
    return models.execute_kw(
        db, uid, api_key,
        'account.move.line', 'read',
        [invoice['line_ids']],
        {'fields': ['name', 'debit', 'credit', 'account_id']}
    )


def process_invoices(models, invoices_to_keep, db, uid, api_key):
    """
    Process invoices, fetch lines, update field and return the data for DataFrame.
    """
    data = []
    code = None
    for invoice in invoices_to_keep:
        move_lines = fetch_lines(models, invoice, db, uid, api_key)
        for line in move_lines:
            try:
                # If no account_id, do nothing
                if line['account_id']:
                    account_information = line['account_id'][1]
                    code = account_information.split(" ", 1)[0]
                    if account_information.startswith('411'):
                        account_name = invoice['partner_id'][1]
                    else:
                        account_name = (
                            line['account_id'][1].split(" ", 1)[1]
                            if " " in line['account_id'][1] else line['account_id'][1]
                        )

                    # Determine type (Invoice or Refund)
                    if 'invoice' in invoice['move_type']:
                        type_facture = f"Facture - {invoice['partner_id'][1]}"
                    elif 'refund' in invoice['move_type']:
                        type_facture = f"Avoir - {invoice['partner_id'][1]}"
                    else:
                        type_facture = 'Inconnu'

                    # Append processed data
                    data.append({
                        'Journal': 'VTE',  # Fixed journal name, e.g., "VTE" for sales
                        'Date': invoice['invoice_date'],  # Invoice date, when the invoice was issued
                        'N piece': invoice['name'],  # Invoice number or unique identifier
                        'Code': code,  # Account code extracted from the account details
                        'Libelle Compte': account_name,  # Account name or client name based on account logic
                        'Libelle': type_facture,
                        # Invoice type (e.g., "Invoice - Client Name" or "Credit Note - Client Name")
                        'Debit': line['debit'],  # Debit amount from the accounting line
                        'Credit': line['credit'],  # Credit amount from the accounting line
                    })
            except Exception as e:
                raise Exception(f"Error processing account_id: {e}")

    # Convert data into DataFrame
    df = pd.DataFrame(data)

    # Convert 'Date' column from yyyy-mm-dd to dd/mm/yyyy
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%d/%m/%Y')

    return df

def mark_csv_as_generated_in_odoo(models, invoices_to_keep, db, uid, api_key):
    for invoice in invoices_to_keep:
        # Update x_studio_csv_gnr to True for processed invoices
        models.execute_kw(
            db, uid, api_key, 'account.move', 'write', [[invoice['id']], {'x_studio_csv_gnr': True}]
        )


def export_csv_and_send_webhook(df):
    """
    Export the DataFrame to a CSV file with a date-prefixed filename and send it via a webhook.

    Parameters:
        df (DataFrame): The pandas DataFrame to export.
    """
    if df.empty:
        print("The DataFrame is empty. No file will be generated or sent.")
        return

    # Generate the filename with the current date as prefix
    current_date = datetime.now().strftime('%Y-%m-%d')
    filename = f"{current_date}_output_invoices.csv"

    # Save the DataFrame to a CSV file
    df.to_csv(filename, index=False)
    print(f"CSV file '{filename}' has been successfully generated.")

    # Prepare the file for the webhook
    files = {
        'file': (filename, open(filename, 'rb'), 'text/csv')
    }

    webhook_url = Variable.get("odoo_automation_webhook_url")

    try:
        # Send the file via the webhook
        response = requests.post(webhook_url, files=files)

        # Check the response
        if response.status_code == 200:
            print(f"The file '{filename}' has been successfully sent to the webhook.")
        else:
            print(f"Failed to send the file: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"An error occurred while sending the file: {e}")


def odoo_invoices_automation_helper():
    connection = BaseHook.get_connection('odoo_connection')
    url = connection.host
    api_key = connection.password
    user_mail = connection.login
    db = connection.schema

    common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
    uid = common.authenticate(db, user_mail, api_key, {})

    models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
    invoices_to_keep = fetch_invoices(models, db, uid, api_key)
    df = process_invoices(models, invoices_to_keep, db, uid, api_key)
    export_csv_and_send_webhook(df)
    mark_csv_as_generated_in_odoo(models, invoices_to_keep, db, uid, api_key)
