def table_columns(cur, table_name):
    sql = "select * from %s where 1=0;" % table_name
    cur.execute(sql)
    return [d[0] for d in cur.description]

def to_objects(columns, records):
    objects = []

    for record in records:
        objects.append({columns[i]: record[i] for i in range(len(columns))})

    return objects
