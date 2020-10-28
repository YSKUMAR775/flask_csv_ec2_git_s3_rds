from flask import Flask, request, jsonify
import pymysql
import csv

sql_flask_excel = Flask(__name__)


@sql_flask_excel.route('/get_data/<int:data>', methods=['Get'])
def fn_11(data):

    a = data
    mydb = pymysql.connect(host='database.c42ojr1a1cpj.ap-south-1.rds.amazonaws.com',
                           user='root',
                           password='yskumar775',
                           db='sql_flask_excel'
                           )
    cur = mydb.cursor()

    query = "select * from sql_flask_excel_table where id = '" + str(a) + "'"

    cur.execute(query)

    s = cur.fetchall()

    total_list = []
    for i in s:

        all_dict = {'id': i[0], 'name_info': i[1], 'mail': i[2], 'contact': i[3], 'address': i[4]}
        total_list.append(all_dict)

    list_columns = ['id', 'name_info', 'mail', 'contact', 'address']
    csv_path = 'C:/Users/Hemanth Y/Desktop/csv_file'

    try:
        with open(csv_path, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=list_columns)
            writer.writeheader()

            for data in total_list:
                writer.writerow(data)
    except IOError:
        print("I/O error")

    # return jsonify(total_list)
    return jsonify({'file_name': csv_path})


if __name__ == '__main__':
    sql_flask_excel.run(host='0.0.0.0')
