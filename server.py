PORT = '8083'
from aiohttp import web
import asyncio
import pandas as pd 
import telebot

data_path	= '/home/dvasilev/projects/log_ttl_report/ttl_aug_03.csv'

def send_photo():
	chat_id = '106129214'
	script_path = '/home/dvasilev/projects/log_ttl_report/'	
	with open(script_path+'token.key','r') as file:
		api_token=file.read().replace('\n', '')
		file.close()
	bot = telebot.TeleBot(api_token)
	photo = open('myplot.png', 'rb')
	bot.send_photo(chat_id, photo)

def get_len(last_string):
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

def get_func(last_string):
	return last_string.split('*')[0]

def get_timers(last_string,step):
	splitters = ['*','a','b','c','d','e','f','g','h','i','j']
	ttl = []
	for splitter in splitters:
		new_string = last_string.split(splitter)
		last_string = new_string[1]
		ttl.append(new_string[0])
	ttl.append(last_string)    
	return int(ttl[step])/1000

def plot_versions(df):
	#for func in df['func'].unique():
		#graphic = df[df.func==func].groupby('AppVersion').median().plot(
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
		#subplots=True,
		figsize=(15,6)
	)
	fig = graphic.get_figure()
	fig.savefig("myplot.png")
	send_photo()

def plot_dates(df):
	#for func in df['func'].unique():
		#graphic = df[df.func==func].groupby('AppVersion').median().plot(
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
		#subplots=True,
		figsize=(15,4)
	)
	fig = graphic.get_figure()
	fig.savefig("myplot.png")
	send_photo()

async def call_check(request):
	return web.Response(text='ok',content_type="text/html")

async def call_log_ttl_report(request):
	# read data
	df = pd.read_csv(data_path,';')
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

# Process calls
async def call_log_ttl_report_test(request):
	result = await request.text()
	return web.Response(text=result,content_type="text/html")
	
app = web.Application()	
#app.router.add_post('/log_ttl_report', call_log_ttl_report)
app.router.add_post('/log_ttl_report', call_log_ttl_report_test)
#app.router.add_route('GET', '/check',	call_check)
#app.router.add_route('GET', '/log_ttl_report',	call_log_ttl_report)

# Start aiohttp server
web.run_app(
    app,
    port=PORT,
)