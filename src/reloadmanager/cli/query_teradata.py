from reloadmanager.clients.teradata_client import TeradataClient


def main(args):
    td_client: TeradataClient = TeradataClient()
    results = td_client.query(args.query, args.headers)
    for row in results:
        print(row)
