#!/usr/bin/env python3
# problem2/api_server.py
import json, sys, argparse, urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
import boto3
from boto3.dynamodb.conditions import Key

def get_table(name, region=None):
    resource = boto3.resource('dynamodb', region_name=region) if region else boto3.resource('dynamodb')
    return resource.Table(name)

def json_response(handler, status, payload):
    data = json.dumps(payload, default=str).encode('utf-8')
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Content-Length', str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)

class Handler(BaseHTTPRequestHandler):
    table_name = "arxiv-papers"
    region = None

    def do_GET(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            qs = urllib.parse.parse_qs(parsed.query)
            parts = [p for p in parsed.path.split('/') if p]
            table = get_table(self.table_name, self.region)

            if parts[:2] == ['papers','recent']:
                category = qs.get('category', ['cs.LG'])[0]
                limit = int(qs.get('limit', ['20'])[0])
                resp = table.query(KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
                                   ScanIndexForward=False, Limit=limit)
                json_response(self, 200, {"category": category, "papers": resp.get('Items',[]), "count": len(resp.get('Items',[]))})

            elif parts[:2] == ['papers','author'] and len(parts) >= 3:
                author_name = urllib.parse.unquote(parts[2])
                resp = table.query(IndexName='AuthorIndex', KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}'))
                json_response(self, 200, {"author": author_name, "papers": resp.get('Items',[]), "count": len(resp.get('Items',[]))})

            elif parts[:1] == ['papers'] and len(parts) == 2:
                arxiv_id = urllib.parse.unquote(parts[1])
                resp = table.query(IndexName='PaperIdIndex', KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}'))
                items = resp.get('Items',[])
                if items:
                    json_response(self, 200, items[0])
                else:
                    json_response(self, 404, {"error": "Paper not found"})

            elif parts[:2] == ['papers','search']:
                category = qs.get('category', ['cs.LG'])[0]
                start = qs.get('start', ['0000-01-01'])[0]
                end   = qs.get('end',   ['9999-12-31'])[0]
                resp = table.query(KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}') & Key('SK').between(f'{start}#', f'{end}#zzzzzzz'))
                json_response(self, 200, {"category": category, "start": start, "end": end, "papers": resp.get('Items',[]), "count": len(resp.get('Items',[]))})

            elif parts[:2] == ['papers','keyword'] and len(parts) >= 3:
                kw = urllib.parse.unquote(parts[2])
                limit = int(qs.get('limit', ['20'])[0])
                resp = table.query(IndexName='KeywordIndex', KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{kw.lower()}'),
                                   ScanIndexForward=False, Limit=limit)
                json_response(self, 200, {"keyword": kw, "papers": resp.get('Items',[]), "count": len(resp.get('Items',[]))})

            else:
                json_response(self, 404, {"error": "Not found"})
        except Exception as e:
            json_response(self, 500, {"error": str(e)})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("port", nargs="?", default=8080, type=int)
    ap.add_argument("--table", default="arxiv-papers")
    ap.add_argument("--region", default=None)
    args = ap.parse_args()
    Handler.table_name = args.table
    Handler.region = args.region
    server = HTTPServer(("0.0.0.0", args.port), Handler)
    print(f"Serving on 0.0.0.0:{args.port} -> table={args.table} region={args.region}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

if __name__ == "__main__":
    main()
