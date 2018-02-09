import asyncio
import async_influxdb


json_body = [
    {
        "measurement": "test_measurement",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "time": "2017-12-11T23:00:00Z",
        "fields": {
            "Float_value": 0.64,
            "Int_value": 3,
            "String_value": "Text",
            "Bool_value": True
        }
    }
]


async def test():
    client = async_influxdb.InfluxDBClient(
        '192.168.90.6', 8086, 'test_user', 'test_password', 'test_db')
    print(await client.ping())
    await client.create_database('test_db2')
    print(await client.get_list_database())
    client.switch_database('test_db2')
    client.switch_database('test_db')
    await client.drop_database('test_db2')
    await client.create_retention_policy('test_policy', '3d', 3, default=True)
    print(await client.get_list_retention_policies())
    await client.drop_retention_policy('test_policy')
    await client.alter_retention_policy('autogen', default=True)
    await client.create_user('test_user2', 'test', admin=True)
    print(await client.get_list_users())
    await client.set_user_password('test_user2', 'test2')
    await client.drop_user('test_user2')
    print(await client.get_list_privileges('test_user'))
    await client.write_points(json_body)
    print(await client.get_list_measurements())
    rs = await client.query("select cpu,usage_idle from cpu limit 3")
    cpu_points = list(rs.get_points(measurement='cpu'))
    for i in cpu_points:
        print(i)
    client.close()


async def main():
    await test()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.stop()
    loop.close()
