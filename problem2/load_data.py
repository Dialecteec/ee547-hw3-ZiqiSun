#!/usr/bin/env python3
# problem2/load_data.py
import argparse, json, re
from collections import Counter
import boto3
from botocore.exceptions import ClientError

STOPWORDS = {
    'the','a','an','and','or','but','in','on','at','to','for','of','with','by','from','up','about','into','through','during',
    'is','are','was','were','be','been','being','have','has','had','do','does','did','will','would','could','should','may','might',
    'can','this','that','these','those','we','our','use','using','based','approach','method','paper','propose','proposed','show'
}

def normalize_kw(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    toks = [t for t in text.split() if t and t not in STOPWORDS and len(t) > 2]
    return toks

def top_keywords(abstract, k=10):
    cnt = Counter(normalize_kw(abstract or ""))
    return [w for w,_ in cnt.most_common(k)]

def create_table(dynamodb, table_name):
    try:
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {'AttributeName':'PK', 'AttributeType':'S'},
                {'AttributeName':'SK', 'AttributeType':'S'},
                {'AttributeName':'GSI1PK', 'AttributeType':'S'},
                {'AttributeName':'GSI2PK', 'AttributeType':'S'},
                {'AttributeName':'GSI3PK', 'AttributeType':'S'},
            ],
            KeySchema=[
                {'AttributeName':'PK', 'KeyType':'HASH'},
                {'AttributeName':'SK', 'KeyType':'RANGE'},
            ],
            GlobalSecondaryIndexes=[
                {'IndexName':'AuthorIndex','KeySchema':[{'AttributeName':'GSI1PK','KeyType':'HASH'},{'AttributeName':'SK','KeyType':'RANGE'}],'Projection':{'ProjectionType':'ALL'},'ProvisionedThroughput':{'ReadCapacityUnits':5,'WriteCapacityUnits':5}},
                {'IndexName':'PaperIdIndex','KeySchema':[{'AttributeName':'GSI2PK','KeyType':'HASH'}],'Projection':{'ProjectionType':'ALL'},'ProvisionedThroughput':{'ReadCapacityUnits':5,'WriteCapacityUnits':5}},
                {'IndexName':'KeywordIndex','KeySchema':[{'AttributeName':'GSI3PK','KeyType':'HASH'},{'AttributeName':'SK','KeyType':'RANGE'}],'Projection':{'ProjectionType':'ALL'},'ProvisionedThroughput':{'ReadCapacityUnits':5,'WriteCapacityUnits':5}},
            ],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={'ReadCapacityUnits':5,'WriteCapacityUnits':5}
        )
        print(f"Creating DynamoDB table: {table_name}")
        dynamodb.get_waiter('table_exists').wait(TableName=table_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table {table_name} already exists.")
        else:
            raise

def batch_write(table, items):
    with table.batch_writer(overwrite_by_pkeys=['PK','SK']) as bw:
        for it in items:
            bw.put_item(Item=it)

def load_papers(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, dict) and 'papers' in data:
        data = data['papers']
    return data

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("papers_json_path")
    ap.add_argument("table_name")
    ap.add_argument("--region", default=None)
    args = ap.parse_args()

    dynamodb = boto3.client('dynamodb', region_name=args.region) if args.region else boto3.client('dynamodb')
    resource = boto3.resource('dynamodb', region_name=args.region) if args.region else boto3.resource('dynamodb')

    create_table(dynamodb, args.table_name)
    table = resource.Table(args.table_name)

    papers = load_papers(args.papers_json_path)
    print(f"Loading {len(papers)} papers from {args.papers_json_path}...")

    total_items = cat_items = author_items = kw_items = id_items = 0

    for p in papers:
        arxiv_id = p.get('id') or p.get('arxiv_id') or p.get('paper_id') or ''
        title = (p.get('title') or '').strip()
        authors = p.get('authors') or p.get('author_list') or []
        abstract = p.get('abstract') or ''
        categories = p.get('categories') or p.get('category_list') or []
        published = p.get('published') or p.get('date') or p.get('updated') or ''

        keywords = top_keywords(abstract, k=10)

        cat_batch = []
        for c in categories:
            cat_batch.append({
                'PK': f'CATEGORY#{c}', 'SK': f'{str(published)[:10]}#{arxiv_id}',
                'arxiv_id': arxiv_id, 'title': title, 'authors': authors,
                'abstract': abstract, 'categories': categories, 'keywords': keywords, 'published': str(published),
            })
        if cat_batch:
            batch_write(table, cat_batch); cat_items += len(cat_batch); total_items += len(cat_batch)

        a_batch = []
        for a in authors:
            a_batch.append({
                'PK': f'CATEGORY#{categories[0]}' if categories else 'CATEGORY#unknown',
                'SK': f'{str(published)[:10]}#{arxiv_id}',
                'GSI1PK': f'AUTHOR#{a}',
                'arxiv_id': arxiv_id, 'title': title, 'authors': authors, 'published': str(published),
            })
        if a_batch:
            batch_write(table, a_batch); author_items += len(a_batch); total_items += len(a_batch)

        pid = {
            'PK': f'CATEGORY#{categories[0]}' if categories else 'CATEGORY#unknown',
            'SK': f'{str(published)[:10]}#{arxiv_id}',
            'GSI2PK': f'PAPER#{arxiv_id}',
            'arxiv_id': arxiv_id, 'title': title, 'authors': authors, 'published': str(published),
        }
        batch_write(table, [pid]); id_items += 1; total_items += 1

        k_batch = []
        for kw in keywords:
            k_batch.append({
                'PK': f'CATEGORY#{categories[0]}' if categories else 'CATEGORY#unknown',
                'SK': f'{str(published)[:10]}#{arxiv_id}',
                'GSI3PK': f'KEYWORD#{kw.lower()}',
                'arxiv_id': arxiv_id, 'title': title, 'authors': authors, 'published': str(published),
            })
        if k_batch:
            batch_write(table, k_batch); kw_items += len(k_batch); total_items += len(k_batch)

    print(f"Created items: total={total_items}, categories={cat_items}, authors={author_items}, keywords={kw_items}, by_id={id_items}")
    if len(papers) > 0:
        print(f"Denormalization factor: {round(total_items/len(papers),2)}x")

if __name__ == '__main__':
    main()
