# -*- coding: utf-8 -*-
#                               ﻢﻴﺣﺮﻟا ﻥﺎﻤﺣﺮﻟا ﻪﻠﻟا ﻢﺳﺎﺑ
#                       ﺮﻳﺪﻗ ءﻲﺷ ﻞﻛ ﻰﻠﻋ ﻮﻫ ﻭ ﻚﻠﻤﻟا ﻪﻟ ﻞﻛﻮﺘﻧ ﻪﻴﻠﻋ
#
#
from func_DASK import rand_weights,exec_time,Usage_CPU_RAM,Post_processing_Weights,ML_Clustering_Asset_Weight
from func_DASK import WorkFlow,Not_delayed_Post_processing_Weights,plot_frontier,DASK_outputs,plot_cumul_return
from config_DASK import*
from Build_dataset import training_set as df_training_set,assets_returns
####################################################################################
exec_time('Started at')
Usage_CPU_RAM()
#"""
#from dask.distributed import Client
#client = Client(n_workers=3, threads_per_worker=1, memory_limit='6GB', silence_logs='error')
#client
#"""

#dask.config.set(scheduler='synchronous')  # overwrite default with single-threaded scheduler
#dask.config.set(scheduler='processes')
print('     Phase 3 : Clustering with MonteCarlo multiple portfolios study')
print('------------------------------------------------------------------------')
print('------------------------------------------------------------------------')
####################################################################################
write_parquet=False
#
client=Client('localhost:8786')
dask.config.set(pool=Pool(proc))
#
print('Number of processes:',proc)
print('-------------------------')
"""
path=os.getcwd()
for files in os.listdir(path):
    if os.path.isfile(os.path.join(path, files)):
        print(files)
"""
####################################################################################
args=n_iter,assets,lst,clusters_lst,scaling,dist,df_training_set,assets_returns
##
lst_portfolios_performances,da_weights,da_portfolios_names,da_Labels=WorkFlow(args)
#DEALLOCATE
del da_Labels
##
dd_Portfolios_perfs=DASK_outputs(
    lst_portfolios_performances,chunk)
####################################################################################
print('------------------------------------------------------------------------')
print('Getting 5 Best VS 5 WORST PORTFOLIOS\n...')
with ProgressBar():
    dd_plot=dd_Portfolios_perfs.nlargest(5, 'cumul_return').append(
        dd_Portfolios_perfs.nsmallest(5, 'cumul_return'))
    #
    df_plot=dd_plot.compute()
    #scheduler="single-threaded")
    Portfolios_to_plot=df_plot.index.to_numpy()
    #
    df_plot['Portfolios_names']=da_portfolios_names[Portfolios_to_plot].compute()
##    
print('FIRST PHASE RESULTS:\n',df_plot)
"""
with ProgressBar():
    print('\n',dd_Portfolios_cumul_return[df_plot_1['Portfolios_names'].to_list()].compute())
"""
#DEALLOCATE MEMORY
del dd_Portfolios_perfs,lst_portfolios_performances,da_portfolios_names
Usage_CPU_RAM()
exec_time('Finished at')
print('-----------------------------------------------------------------------')
####################################################################################
####################################################################################
####################################################################################
print('-----------------------------------------------------------------------') ###
print('-----------------------------------------------------------------------') ###
print('Second Phase: Optimizing Best Portfolio\n...')                            ###
####################################################################################
Best_portfolio_name=df_plot['Portfolios_names'].values[0]
lst_Best_portfolio_name=Best_portfolio_name.split('_')
##
idx_Best_weight=lst_Best_portfolio_name[0]
Best_scaling=lst_Best_portfolio_name[1]
Best_dist=lst_Best_portfolio_name[2]
##
Best_weight=da_weights[int(idx_Best_weight)].compute()
arr_max=Best_weight.max(axis=0)
arr_max[arr_max==0]=1
#
Best_lst=[[int(low_reduce*w_min),int(high_reduce*w_max)] for w_min,w_max in zip(
    Best_weight.min(axis=0),arr_max)]
##
args=m_iter,assets,Best_lst,clusters_lst,np.array([int(Best_scaling)]),[Best_dist],df_training_set,assets_returns
##
lst_portfolios_performances,da_weights,da_portfolios_names,da_Labels=WorkFlow(args)
##
dd_Portfolios_perfs=DASK_outputs(
    lst_portfolios_performances,chunk)
####################################################################################
print('------------------------------------------------------------------------')
print('Getting 5 Best VS 5 WORST PORTFOLIOS\n...')
with ProgressBar():
    dd_plot=dd_Portfolios_perfs.nlargest(5, 'cumul_return').append(
        dd_Portfolios_perfs.nsmallest(5, 'cumul_return'))
    #
    df_plot=dd_plot.compute()
    #scheduler="single-threaded")
    Portfolios_to_plot=df_plot.index.to_numpy()
    #
    df_plot['Portfolios_names']=da_portfolios_names[Portfolios_to_plot].compute()
print('------------------------------------------------------------------------')
####################################################################################    
print('SECOND PHASE RESULTS:\n',df_plot)
print('Number of cases:',m_iter)
#
Labels=[da.squeeze(da_Labels[k]).compute() for k in Portfolios_to_plot ]
#DEALLOCATE MEMORY
del da_Labels
##
lst_dfs=list(map(Not_delayed_Post_processing_Weights,
    m_iter*[assets],
    Labels,
    da_weights[Portfolios_to_plot].compute(),
    m_iter*[assets_returns]
    ))

lst_dfs_returns,lst_dfs_cumuls=zip(*lst_dfs)
#
df_final_cumul=pd.concat(lst_dfs_cumuls,axis=1)
df_final_cumul.columns=df_plot['Portfolios_names'].values
#
df_final_returns=pd.concat(lst_dfs_returns,axis=1)
df_final_returns.columns=df_plot['Portfolios_names'].values
#
"""
with ProgressBar():
    print('\n',dd_Portfolios_cumul_return[df_plot_1['Portfolios_names'].to_list()].compute())
"""
Usage_CPU_RAM()
print('------------------------------------------------------------------------')
####################################################################################
print('PLOTTTING FINAL RESULTS:\n...')
"""
filename='../../Market_Data.h5'
market_data = pd.HDFStore(filename,'r')
#
bench_cumul_return=market_data['Cumulative_Returns'][
market_data['Cumulative_Returns'].index>=startD][['SPY','Gold']]
#
bench_cumul_return-=bench_cumul_return.values[0]
#plot_cumul_return(df_final_cumul,bench_cumul_return)
#
market_data.close()
"""

df_final_cumul.index=df_training_set.index
df_final_returns.index=df_training_set.index
####################################################################################
#df_plot_scatter=dd_Portfolios_perfs.nlargest(1000, 'cumul_return').compute()
#plot_frontier(df_plot_scatter)
#DEALLOCATE MEMORY
del df_plot_scatter
Usage_CPU_RAM()
print('------------------------------------------------------------------------')
####################################################################################
print('BEST PARAMETERS:\n')
#
df_Best_Labels=pd.DataFrame(Labels[0].astype(int),index=np.arange(len(Labels[0])),columns=['Labels'])
df_Best_Labels.index=df_final_cumul.index
df_count=df_Best_Labels.value_counts().to_frame()
df_count.columns=['Labels Count']
#
df_Best_weight=pd.DataFrame(Best_weight,columns=assets).astype(int)
print('BEST WEIGHTS:\n',df_Best_weight)
print('BEST Labels count:\n',df_count)
os.chdir('../../')
#
df_Best_Labels.columns=[str(Best_scaling)]
#df_Best_Labels.set_index('Date',inplace=True)
#
store_data = pd.HDFStore('Best_Parameters.h5',mode='w')

store_data.put('Returns',df_final_returns,format="table")
store_data.put('Cumulative_Returns',df_final_cumul,format="table")
#
store_data.put('Best_weights',df_Best_weight,format="table")
store_data.put('Best_Labels',df_Best_Labels,format="table")
#store_data.put('Labels_Count',df_count,format="table")
#
#
store_data.close()
#
#df_Best_weight.to_csv('Best_Weights.csv')

print('------------------------------------------------------------------------')
####################################################################################
client.shutdown()
#
"""
workers = list(client.scheduler_info()['workers'])
client.run_on_scheduler(lambda dask_scheduler=None: 
    dask_scheduler.retire_workers(workers, close_workers=True))
"""
Usage_CPU_RAM()
exec_time('Finished at')
print('*******************************END*******************************')
print('*****************************************************************')

#
#
#
#
#