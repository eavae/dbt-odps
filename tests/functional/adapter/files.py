seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()

seeds_base_name_updated = """
id,name,some_date
1,Easton_updated,1981-05-20T06:46:51
2,Lillian_updated,1978-09-03T18:10:33
3,Jeremiah_updated,1982-03-11T03:59:51
4,Nolan_updated,1976-05-06T20:21:35
5,Hannah_updated,1982-06-23T05:41:26
6,Eleanor_updated,1991-08-10T23:12:21
7,Lily_updated,1971-03-29T14:58:02
8,Jonathan_updated,1988-02-26T02:55:24
9,Adrian_updated,1994-02-09T13:14:23
10,Nora_updated,1976-03-01T16:51:39
""".lstrip()

seeds_base_newcolumns_added_1hour_csv = """
id,name,some_date,last_initial
1,Easton,1981-05-20T07:46:51,A
2,Lillian,1978-09-03T19:10:33,B
3,Jeremiah,1982-03-11T04:59:51,C
4,Nolan,1976-05-06T21:21:35,D
5,Hannah,1982-06-23T06:41:26,E
6,Eleanor,1991-08-11T00:12:21,F
7,Lily,1971-03-29T15:58:02,G
8,Jonathan,1988-02-26T03:55:24,H
9,Adrian,1994-02-09T14:14:23,I
10,Nora,1976-03-01T17:51:39,J
""".lstrip()

seeds_base_newcolumns_name_updated_csv = """
id,name,some_date,last_initial
1,Easton_updated,1981-05-20T06:46:51,A
2,Lillian_updated,1978-09-03T18:10:33,B
3,Jeremiah_updated,1982-03-11T03:59:51,C
4,Nolan_updated,1976-05-06T20:21:35,D
5,Hannah_updated,1982-06-23T05:41:26,E
6,Eleanor_updated,1991-08-10T23:12:21,F
7,Lily_updated,1971-03-29T14:58:02,G
8,Jonathan_updated,1988-02-26T02:55:24,H
9,Adrian_updated,1994-02-09T13:14:23,I
10,Nora_updated,1976-03-01T16:51:39,J
""".lstrip()

seeds_added = """
11,Mateo,2014-09-07T17:04:27
12,Julian,2000-02-04T11:48:30
13,Gabriel,2001-07-10T07:32:52
14,Isaac,2002-11-24T03:22:28
15,Levi,2009-11-15T11:57:15
16,Elizabeth,2005-04-09T03:50:11
17,Grayson,2019-08-06T19:28:17
18,Dylan,2014-03-01T11:50:41
19,Jayden,2009-06-06T07:12:49
20,Luke,2003-12-05T21:42:18
""".lstrip()

seeds_added_1hour_csv = """
11,Mateo,2014-09-07T18:04:27
12,Julian,2000-02-04T12:48:30
13,Gabriel,2001-07-10T08:32:52
14,Isaac,2002-11-24T04:22:28
15,Levi,2009-11-15T12:57:15
16,Elizabeth,2005-04-09T04:50:11
17,Grayson,2019-08-06T20:28:17
18,Dylan,2014-03-01T12:50:41
19,Jayden,2009-06-06T08:12:49
20,Luke,2003-12-05T22:42:18
""".lstrip()

seeds_all_added_1hour_csv = seeds_base_csv + seeds_added_1hour_csv
seeds_name_updated_csv = seeds_base_name_updated + seeds_added_1hour_csv
