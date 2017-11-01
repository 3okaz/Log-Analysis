import psycopg2

# queries

# popular three articles

query_1_title = ("popular three articles")
query_1 = (
    "select articles.title, count(*) as views "
    "from articles inner join log on log.path "
    "like concat('%', articles.slug, '%') "
    "where log.status like '%200%' group by "
    "articles.title, log.path order by views desc limit 3")

# popular article authors
title2 = ("popular article authors")
query_2 = (
    "select authors.name, count(*) as views from articles inner "
    "join authors on articles.author = authors.id inner join log "
    "on log.path like concat('%', articles.slug, '%') where "
    "log.status like '%200%' group "
    "by authors.name order by views desc")

# errors
title3 = ("errors")
query_3 = (
    "select day, perc from ("
    "select day, round((sum(requests)/(select count(*) from log where "
    "substring(cast(log.time as text), 0, 11) = day) * 100), 2) as "
    "perc from (select substring(cast(log.time as text), 0, 11) as day, "
    "count(*) as requests from log where status like '%404%' group by day)"
    "as log_percentage group by day order by perc desc) as final_query "
    "where perc >= 1")

# connect to postgreSql Database

def connect(database_name="news"):
    """Connect to the PostgreSQL database. Returns a database connection """
    try:
        database = psycopg2.connect("dbname={}".format(database_name))
        cursor = database.cursor()
        return database, cursor
    except:
        print ("can not to connect to the database")
        sys.exit(1)


def get_query_results(query):
    """Return query results"""
    database, cursor = connect()
    cursor.execute(query)
    database.close()
    return cursor.fetchall()

def print_error(query_results):
    print (query_results[1])
    for results in query_results[0]:
        print ("\t\t", results[0], "-", str(results[1]) + "% errors")


def print_query(query_results):
    print (query_results[1])
    for index, results in enumerate(query_results[0]):
        print (
            "\t\t", index+1, "-", results[0],
            "\t\t - ", str(results[1]), "views")

if __name__ == '__main__':
    
# for store
    popular_articles = get_query_results(query_1), title1
    popular_authors = get_query_results(query_2), title2
    error_days = get_query_results(query_3), title3
# for print
    print_results(popular_articles)
    print_results(popular_authors)
    print_error(error_days)


    
