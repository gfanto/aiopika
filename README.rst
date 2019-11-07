Aiopika
========
  Asyncronus version of the popular RabbitMQ (AMQP 0-9-1) python client library pika.

Introduction
------------
Aiopika is a pure-python asyncronus implementation of the AMQP 0-9-1 protocol.
Only python 3.6+ are supported
It's meant to be used as "drop-in" replacement of the original pika using the async/await syntax, to speed up you code.
To boost performance even more you can install uvloop and this will be enable automatically at the import.

Example
-------
Simple publisher example:

.. code :: python

	from aiopika import BlockingConnection
	async with BlockingConnection() as conn:
		ch = conn.channel()
		async with ch:
			await ch.queue_declare('myqueue', passive=True)
			await ch.basic_publish('', 'myqueue', b'Hello, World!')

An example of writing a blocking consumer:

.. code :: python

	import signal
	import asyncio
	from functools import partial
	from aiopika import BlockingConnection

	def handle_sigint(channel):
		asyncio.create_task(channel.stop_consuming())

	async def on_message(channel, method, header, body):
		print(body.decode('utf-8'))
		await channel.basic_ack(method.delivery_tag)

	loop = asyncio.get_event_loop()
	async with BlockingConnection() as conn:
		async with (await conn.channel()) as ch:
			loop.add_signal_handler(signal.SIGINT, partial(handle_sigint, ch))
			await ch.basic_consume('myqueue', on_message)
			await ch.start_consuming()

Documentation
-------------
TODO

