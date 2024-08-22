import requests

class GraphQLClient:
    def __init__(self, url):
        self.url = url

    def send_query(self, query, variables):
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(
                self.url, json={"query": query, "variables": variables}, headers=headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error sending GraphQL query: {e}")
            return None
