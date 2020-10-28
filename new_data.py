from flask import Flask, jsonify, request
import boto3
import pymysql


new_data = Flask(__name__)


@new_data.route('/postal', methods=['Post'])
def read():
    info = request.get_json()
    file_path = info['file_path']

    ACCESS_KEY_ID = 'AKIAXB3Y4CS3GX34M5OS'
    ACCESS_SECRET_KEY = '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'
    AWS_DEFAULT_REGION = 'ap-south-1'
    BUCKET_NAME = 'kumar776'

    s3_cli = boto3.resource(
        's3',
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_SECRET_KEY,
        region_name=AWS_DEFAULT_REGION,
        # config=Config(signature_version='s3v4')
        )
    my_bucket = s3_cli.Bucket(BUCKET_NAME)

    obj = my_bucket.Object(key=file_path)
    response = obj.get()
    encoding = 'utf-8'
    lines = response['Body'].read().decode(encoding).split('\n')
    # print(lines)

    old_list = []
    for i in lines:
        a = i.strip()
        old_list.append(a)
    del old_list[0]        # we can use pop or del methods
    old_list.pop(-1)
    # print(old_list)

    new_list = []
    for i in old_list:
        x = i.split(',')
        # print(x)

        id = x[0]
        period = x[1]
        short_descriptions = x[2]
        temperatures = x[3]

        # a = id.replace(' ', '')
        # b = period.replace(' ', '')
        # c = short_descriptions.replace(' ', '')
        # d = temperatures.replace(' ', '')

        dict_data = {"id": id, "period": period, "short_descriptions": short_descriptions, "temperatures": temperatures}
        new_list.append(dict_data)

    mydb = pymysql.connect(host='database.c42ojr1a1cpj.ap-south-1.rds.amazonaws.com',
                           user='root',
                           password='yskumar775',
                           db='aws3'
                           )
    cur = mydb.cursor()

    # for i in new_list:
    #     a = i['id']
    #     b = i['period']
    #     c = i['short_descriptions']
    #     d = i['temperatures']
    #
    #     query_1 = "insert into aws3_table values('" + str(a) + "','" + str(b) + "','" + str(c) + "','" + str(d) + "')"
    #     cur.execute(query_1)
    #
    # mydb.commit()
    # return jsonify({'url': file_path})

    query_2 = "select * from aws3_table"
    cur.execute(query_2)
    s = cur.fetchall()

    total_list = []
    for i in s:
        all_dict = {'id': i[0], 'period': i[1], 'short_descriptions': i[2], 'temperatures': i[3]}
        total_list.append(all_dict)

    print(total_list)

    return jsonify(total_list)


if __name__ == "__main__":
    new_data.run(host='0.0.0.0')
