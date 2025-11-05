#!/usr/bin/env python3
# problem2/query_papers.py
import argparse, json, time
import boto3
from boto3.dynamodb.conditions import Key

def get_table(name, region=None):
    resource = boto3.resource('dynamodb', region_name=region) if region else boto3.resource('dynamodb')
    return resource.Table(name)

def out(query_type, params, items, t0):
    print(json.dumps({
        "query_type": query_type,
        "parameters": params,
        "results": items,
        "count": len(items),
        "execution_time_ms": round((time.time()-t0)*1000,2)
    }, default=str))

def query_recent_in_category(table, category, limit):
    t0=time.time()
    resp = table.query(KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'), ScanIndexForward=False, Limit=limit)
    out("recent_in_category", {"category":category, "limit":limit}, resp.get('Items',[]), t0)

def query_papers_by_author(table, author):
    t0=time.time()
    resp = table.query(IndexName='AuthorIndex', KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author}'))
    out("papers_by_author", {"author":author}, resp.get('Items',[]), t0)

def get_paper_by_id(table, arxiv_id):
    t0=time.time()
    resp = table.query(IndexName='PaperIdIndex', KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}'))
    out("get_by_id", {"arxiv_id":arxiv_id}, resp.get('Items',[]), t0)

def query_papers_in_date_range(table, category, start_date, end_date):
    t0=time.time()
    resp = table.query(KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}') & Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz'))
    out("date_range", {"category":category,"start_date":start_date,"end_date":end_date}, resp.get('Items',[]), t0)

def query_papers_by_keyword(table, keyword, limit):
    t0=time.time()
    resp = table.query(IndexName='KeywordIndex', KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
                       ScanIndexForward=False, Limit=limit)
    out("keyword", {"keyword":keyword, "limit":limit}, resp.get('Items',[]), t0)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["recent","author","get","daterange","keyword"])
    ap.add_argument("arg1")
    ap.add_argument("arg2", nargs='?')
    ap.add_argument("arg3", nargs='?')
    ap.add_argument("--table", default="arxiv-papers")
    ap.add_argument("--region", default=None)
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()
    table = get_table(args.table, args.region)

    if args.mode == "recent":
        query_recent_in_category(table, args.arg1, args.limit)
    elif args.mode == "author":
        query_papers_by_author(table, args.arg1)
    elif args.mode == "get":
        get_paper_by_id(table, args.arg1)
    elif args.mode == "daterange":
        if args.arg2 is None or args.arg3 is None:
            raise SystemExit("daterange requires <category> <start_date> <end_date>")
        query_papers_in_date_range(table, args.arg1, args.arg2, args.arg3)
    elif args.mode == "keyword":
        query_papers_by_keyword(table, args.arg1, args.limit)

if __name__ == "__main__":
    main()
