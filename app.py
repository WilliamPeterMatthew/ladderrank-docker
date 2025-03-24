import pymongo
from flask import Flask, jsonify, request
from bson.objectid import ObjectId
from datetime import datetime
import os
import yaml

app = Flask(__name__)


DOCKER_PORT = 5000  # 暴露的端口，保持不变

def get_mongo_client():
    """
    获取MongoDB客户端。
    尝试从环境变量获取MONGO_HOST，如果不存在，则使用'oj-mongo'。
    """
    mongo_host = os.environ.get('MONGO_HOST', 'oj-mongo')
    mongo_port = int(os.environ.get('MONGO_PORT', '27017')) # 显式转换为整数，处理可能缺失的情况
    try:
        client = pymongo.MongoClient(host=mongo_host, port=mongo_port, serverSelectionTimeoutMS=5000) #设置超时
        client.admin.command('ping')  # 检查连接是否成功
        return client
    except pymongo.errors.ConnectionFailure as e:
        print(f"Failed to connect to MongoDB: {e}")
        raise  # 抛出异常，以便上层处理

def close_mongo_client(client):
    """
    关闭MongoDB客户端连接。
    """
    if client:
        client.close()

@app.route('/hydro/document', methods=['GET'])
def get_documents():
    """
    1. 通过API接口请求hydro数据库document集合的数据，会给出domainId（字符串）的值，
    返回集合中所有domainId等于传入值且docType为30的数据。
    只返回对应数据的_id（ObjectId("")类型）,docId（ObjectId("")类型）,title（文本）,
    beginAt（ISODate("")类型）,pids（数字列表）字段。

    2. 通过API接口请求hydro数据库document集合的数据，会给出domainId（字符串）的值，
    返回集合中所有domainId等于传入值且docType为10的数据。
    只返回对应数据的_id（ObjectId("")类型）,docId（数字）,title（文本）,pid（字符串）,
    config（字符串保存的yaml格式的文件）字段。
    """
    client = None # 初始化client
    try:
        client = get_mongo_client()
        db = client.hydro
        domain_id = request.args.get('domainId')
        doc_type = int(request.args.get('docType', 0)) # 增加docType参数，并提供默认值0

        if not domain_id:
            return jsonify({'error': 'domainId is required'}), 400

        if doc_type == 30:
            documents = db.document.find(
                {'domainId': domain_id, 'docType': 30},
                {'_id': 1, 'docId': 1, 'title': 1, 'beginAt': 1, 'pids': 1}
            )
            result = [{
                '_id': str(doc['_id']),
                'docId': str(doc.get('docId')), # docId可能是ObjectId
                'title': doc['title'],
                'beginAt': doc.get('beginAt'), # beginAt可能不存在
                'pids': doc.get('pids', [])  # pids可能不存在
            } for doc in documents]
        elif doc_type == 10:
            documents = db.document.find(
                {'domainId': domain_id, 'docType': 10},
                {'_id': 1, 'docId': 1, 'title': 1, 'pid': 1, 'config': 1}
            )
            result = []
            for doc in documents:
                try:
                    config_data = yaml.safe_load(doc['config'])
                    total_score = 0
                    if config_data and 'subtasks' in config_data: # 检查 config_data 是否为 None
                        for subtask in config_data['subtasks']:
                            total_score += subtask.get('score', 0)  # 避免子任务中没有 'score' 字段的情况
                    result.append({
                        '_id': str(doc['_id']),
                        'docId': doc['docId'],
                        'title': doc['title'],
                        'pid': doc['pid'],
                        "score": total_score
                    })
                except yaml.YAMLError as e:
                    print(f"Error parsing YAML config: {e}")
                    # 处理 YAML 解析错误，例如，返回错误消息或跳过此文档
                    result.append({
                        '_id': str(doc['_id']),
                        'docId': doc['docId'],
                        'title': doc['title'],
                        'pid': doc['pid'],
                        "score": 0,
                        "error": "Invalid config format" # 可以添加一个错误标记
                    })

        else:
            return jsonify({'error': 'Invalid docType'}), 400 # docType错误，返回400

        return jsonify(result), 200
    except pymongo.errors.ConnectionFailure:
        return jsonify({'error': 'Failed to connect to MongoDB'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_mongo_client(client)

@app.route('/hydro/record', methods=['GET'])
def get_records():
    """
    2. 通过API接口请求hydro数据库record集合的数据，会给出domainId（字符串）和contest（ObjectId("")类型）的值，
    返回集合中所有domainId和contest等于传入值的数据。
    只返回对应数据的_id（ObjectId("")类型）,status（数字）,uid（数字）,pid（数字）,score（数字）,
    judgeAt（ISODate("")类型）字段。
    """
    client = None # 初始化
    try:
        client = get_mongo_client()
        db = client.hydro
        domain_id = request.args.get('domainId')
        contest_id_str = request.args.get('contest')

        if not domain_id:
            return jsonify({'error': 'domainId is required'}), 400
        if not contest_id_str:
            return jsonify({'error': 'contest is required'}), 400

        try:
            contest_id = ObjectId(contest_id_str)
        except Exception:
            return jsonify({'error': 'Invalid contest ObjectId'}), 400

        records = db.record.find(
            {'domainId': domain_id, 'contest': contest_id},
            {'_id': 1, 'status': 1, 'uid': 1, 'pid': 1, 'score': 1, 'judgeAt': 1}
        )
        result = [{
            '_id': str(record['_id']),
            'status': record['status'],
            'uid': record['uid'],
            'pid': record['pid'],
            'score': record['score'],
            'judgeAt': record.get('judgeAt') # judgeAt可能不存在
        } for record in records]
        return jsonify(result), 200
    except pymongo.errors.ConnectionFailure:
        return jsonify({'error': 'Failed to connect to MongoDB'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_mongo_client(client)

@app.route('/hydro/user', methods=['GET'])
def get_user():
    """
    4. 通过API接口请求hydro数据库user集合的数据，会给出_id（数字）的值，
    返回集合中_id等于传入值的唯一数据。只返回对应数据的uname（字符串）字段。
    """
    client = None
    try:
        client = get_mongo_client()
        db = client.hydro
        user_id = request.args.get('_id')

        if not user_id:
            return jsonify({'error': '_id is required'}), 400

        if not user_id.isdigit():
            return jsonify({'error': '_id must be an integer'}), 400

        user = db.user.find_one(
            {'_id': int(user_id)},  # 确保user_id是整数
            {'uname': 1}
        )
        if user:
            result = {'uname': user['uname']}
            return jsonify(result), 200
        else:
            return jsonify({'error': 'User not found'}), 404
    except pymongo.errors.ConnectionFailure:
        return jsonify({'error': 'Failed to connect to MongoDB'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_mongo_client(client)

@app.route('/hydro/user/group', methods=['GET'])
def get_user_groups():
    """
    5. 通过API接口请求hydro数据库user.group集合的数据，会给出domainId（字符串）的值，
    返回集合中所有domainId等于传入值的数据。只返回对应数据的_id（ObjectId("")类型）,
    name（文本）,uids（数字列表）字段。
    """
    client = None
    try:
        client = get_mongo_client()
        db = client.hydro
        domain_id = request.args.get('domainId')

        if not domain_id:
            return jsonify({'error': 'domainId is required'}), 400

        user_groups = db['user.group'].find( # 使用db[collection_name]的方式访问集合
            {'domainId': domain_id},
            {'_id': 1, 'name': 1, 'uids': 1}
        )
        result = [{
            '_id': str(group['_id']),
            'name': group['name'],
            'uids': group.get('uids', []) # 确保返回一个列表，即使数据库中没有
        } for group in user_groups]
        return jsonify(result), 200
    except pymongo.errors.ConnectionFailure:
        return jsonify({'error': 'Failed to connect to MongoDB'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_mongo_client(client)

if __name__ == "__main__":
    # 尝试从文件中加载配置
    config_file = "config.yaml"
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
                if config: # 检查config是否为空
                    # 更新MONGO_HOST和DOCKER_PORT，如果配置文件中有
                    MONGO_HOST = config.get('mongo_host', os.environ.get('MONGO_HOST', 'oj-mongo'))
                    DOCKER_PORT = config.get('docker_port', DOCKER_PORT)
                    os.environ['MONGO_HOST'] = MONGO_HOST #设置环境变量
                    print(f"Using MONGO_HOST from {config_file}: {MONGO_HOST}")
                    print(f"Using DOCKER_PORT from {config_file}: {DOCKER_PORT}")
        except yaml.YAMLError as e:
            print(f"Error reading config file {config_file}: {e}")
            # 不终止程序，继续使用环境变量或默认值
        except Exception as e:
            print(f"An unexpected error occurred while reading config file: {e}")
    else:
        print(f"Config file {config_file} not found, using environment variables or defaults.")
    try:
        app.run(host='0.0.0.0', port=DOCKER_PORT) # 运行Flask应用
    except Exception as e:
        print(f"An error occurred: {e}")
