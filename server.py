PORT = '8083'
from aiohttp import web
import asyncio
import pandas as pd 
import telebot

def send_photo():
	script_path = '/home/dvasilev/projects/log_ttl_report/'
	chat_id = '-342878753' # mrm events
	
	with open(script_path+'token.key','r') as file:
		api_token=file.read().replace('\n', '')
		file.close()
	bot = telebot.TeleBot(api_token)
	photo = open(script_path+'myplot.png', 'rb')
	bot.send_photo(chat_id, photo)

def get_len(last_string):
	try:
		splitters = ['*','a','b','c','d','e','f','g','h','i','j']
		ttl = []
		for splitter in splitters:
			new_string = last_string.split(splitter)
			last_string = new_string[1]
			ttl.append(new_string[0])
		ttl.append(last_string)
		first_value = int(ttl[2])
		last_value = int(ttl[-2:][0])
		return last_value-first_value
	except Exception as e:
		return 0

def get_func(last_string):
	return last_string.split('*')[0]

def get_timers(last_string,step):
	try:
		splitters = ['*','a','b','c','d','e','f','g','h','i','j']
		ttl = []
		for splitter in splitters:
			new_string = last_string.split(splitter)
			last_string = new_string[1]
			ttl.append(new_string[0])
		ttl.append(last_string)    
		return int(ttl[step])/1000
	except Exception as e:
		return 0

def plot_versions(df):
	script_path = '/home/dvasilev/projects/log_ttl_report/'
	graphic = df.groupby('AppVersion').median().plot(
		y=[
			'ab_mrm_to_back',
			'bc_back_to_back',
			'cd_back_to_1c',
			#'de_1c_to_1c',
			'ef_1c_to_back',
			'fg_back_to_back',
			'gh_back_to_mrm',
			'hi_mrm_to_mrm',
		],
		kind='bar',
		title = 'log ttl: version',
		figsize=(15,6)
	)
	fig = graphic.get_figure()
	fig.savefig(script_path+"myplot.png")
	send_photo()

def plot_dates(df):
	script_path = '/home/dvasilev/projects/log_ttl_report/'
	graphic = df.groupby('day').median().plot(        
		y=[
			'ab_mrm_to_back',
			'bc_back_to_back',
			'cd_back_to_1c',
			'de_1c_to_1c',
			'ef_1c_to_back',
			'fg_back_to_back',
			'gh_back_to_mrm',
			'hi_mrm_to_mrm',
		],
		kind='bar',
		title = 'log ttl: date',
		figsize=(15,6)
	)
	fig = graphic.get_figure()
	fig.savefig(script_path+"myplot.png")
	send_photo()

async def call_log_ttl_report(request):
	
	script_path = '/home/dvasilev/projects/log_ttl_report/'
	
	# save data
	with open(script_path+'data.csv', 'w') as file: # change filename to token if call can be parallel
		file.write(await request.text())
	
	# read data
	df = pd.read_csv(script_path+'data.csv',';')
	df.fillna(0, inplace=True)

	# ttl
	df['dev_len']=df['ttl'].apply(get_len)

	# functions
	df['func']=df['ttl'].apply(get_func)

	# stage timers
	timer_columns = ['a','b','c','d','e','f','g','h','i','j']
	for i in range(0,len(timer_columns)):
		column_name = timer_columns[i]
		df[column_name]=df['ttl'].apply(get_timers,step=i+2)

	# top & bottom bias, each phone
	for phone in df["phone"].unique():
		mr = df[df.phone==phone].sort_values(by=['dev_len']).iloc[0] #minimal delay record
		bias_top=(mr.h-mr.a-(mr.g-mr.b))/2-(mr.b-mr.a)
		bias_bottom=(mr.f+bias_top-(mr.c+bias_top)-(mr.e-mr.d))/2-(mr.d-(mr.c+bias_top))
		df.loc[df['phone'] == phone, 'bias_top'] = bias_top    
		df.loc[df['phone'] == phone, 'bias_bottom'] = bias_bottom

	# delay between instances
	df['ab_mrm_to_back']      = df.b - df.a + df.bias_top    
	df['bc_back_to_back']     = df.c - df.b
	df['cd_back_to_1c']       = df.d - df.c + df.bias_bottom - df.bias_top
	df['de_1c_to_1c']         = df.e - df.d
	df['ef_1c_to_back']       = df.f - df.e + df.bias_top - df.bias_bottom
	df['fg_back_to_back']     = df.g - df.f
	df['gh_back_to_mrm']      = df.h - df.g - df.bias_top
	df['hi_mrm_to_mrm']       = df.i - df.h

	# plot
	plot_versions(df)
	df['day'] = df['date'].str.split().str[0]
	plot_dates(df)
	return web.Response(text='ok',content_type="text/html")
	
app = web.Application(client_max_size=1024**3)	
app.router.add_post('/log_ttl_report', call_log_ttl_report)

# Start aiohttp server
web.run_app(
    app,
    port=PORT,
)