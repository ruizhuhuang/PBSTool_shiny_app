# install.packages("arulesViz")
# install.packages("RColorBrewer")
# install.packages("data.table")
# http://web.corral.tacc.utexas.edu/XALT/
# xalt: /work/00791/xwj/DMS/R-training/RBigData/shiny/xalt_shiny_app
# runApp("/work/03076/rhuang/wrangler/PBSTool/PBSTool_shiny_app/")


timeRange = c("2014-07", "2014-08", "2014-09", "2014-10", "2014-11", "2014-12",
              "2015-01", "2015-02", "2015-03", "2015-04", "2015-05", "2015-06")
# c("_corrupt_record","allocation","build_date","build_user","date","exec_path",
#   "field_of_science","host","job_id","linkA","link_program","module_name",
#   "num_cores","num_nodes","num_threads","run_time","start_time","user")
column_names = c("jobid","system","username","groupname","account","submithost",
                 "jobname","nproc","mppe","mppssp","nodes","nodect","feature",
                 "gres","queue","qos","submit_ts","submit_date","start_ts",
                 "start_date","end_ts","end_date","cput_req","cput","walltime_req",
                 "walltime","mem_req","mem_kb","vmem_req","vmem_kb","energy",
                 "software","hostlist","exit_status","script","sw_app","contact",
                 "pmem_req")

log_data_dir = "/work/00791/xwj/XALT/GIT/"
# tmp_dir to store symbolic link for selection of time perild
#tmp_dir = sprintf("%s/xalt_tmp", Sys.getenv("HOME"));
tmp_dir = "/work/00791/xwj/XALT/GIT/"
#out_dir = sprintf("%s/xalt_out", Sys.getenv("HOME"));
out_dir = sprintf("%s/test", Sys.getenv("HOME"));

create_dir_cmd=sprintf("rm -rf %s;mkdir %s;chmod -R a+rx %s",out_dir,out_dir,out_dir)
system(create_dir_cmd)  # must be absolute path
pmml_path = sprintf("%s/rules.pmml.xml", out_dir)
dat=list()

get_file_names=function(time1,time2){
  s=which(timeRange==time1)
  e=which(timeRange==time2)
  selected = timeRange[s:e]
  file_names=paste("xalt-",selected,".json",sep="",collapse=",")
  return (file_names)
}

create_symbolic_link_for_sel_in_a_tmp_directory=function(log_data_dir, tmp_dir,file_names){
  create_dir_cmd=sprintf("rm -rf %s;mkdir %s;chmod a+rx %s",tmp_dir,tmp_dir,tmp_dir)
  system(create_dir_cmd)
  num_files=length(unlist(strsplit(file_names,split = ",")))
  if(num_files==1){
    ln_cmd=sprintf("ln -s %s/%s %s", log_data_dir, file_names,tmp_dir)
  }else{
    ln_cmd=sprintf("ln -s %s/{%s} %s", log_data_dir, file_names,tmp_dir)
  }
  system(ln_cmd)
}


runXaltJava = function(StartMonth, EndMonth,conf, sup,output,
                       column_dis_b ,column_dis_v,
                       association_b,association_v,
                       top_m,rules,plot_type){
#  files_sel=get_file_names(StartMonth, EndMonth)
#  create_symbolic_link_for_sel_in_a_tmp_directory(log_data_dir, tmp_dir,files_sel)
  b = which (column_names==column_dis_b)-1
  v = (which (column_names==column_dis_v)-1)
  as_b= which (column_names==association_b)-1
  t= as.character(as.integer(association_v))
  as_v_b = paste(as.character(append(t,as.character(as_b))), collapse = ",")
  print(b)
  print(v)
  print(t)
  print(as_v_b)
  Sys.sleep(6)
  
  submit_cmd_1 = sprintf("/home/03076/rhuang/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class apps.PBSToolLog --master local[16] /work/00791/xwj/XALT/loganalyzer.jar -b %s -v %s,%s -f %s -out %s/test -d ",
                       b,v,b,
                       log_data_dir, Sys.getenv("HOME"))

  
#  --executor-cores 1
   submit_cmd_2 = sprintf("/home/03076/rhuang/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class apps.PBSToolLog --master local[16]  /work/00791/xwj/XALT/loganalyzer.jar -a -b %s -v %s -conf %s -sup %s -f %s -out %s/test ",
                          as_b, as_v_b,
                          conf, sup, log_data_dir, Sys.getenv("HOME"))

  time0=proc.time()
  Sys.setenv(SPARK_MASTER="local[16]")
  system(submit_cmd_1)
  #print(proc.time()-time0)
  time1=proc.time()-time0
  
  time0=proc.time()
#  system(submit_cmd_2)
  #print(proc.time()-time0)
  time2=proc.time()-time0
  print(time1)
  print(time2)
  #system(sprintf("rm -rf %s/xalt_tmp/", Sys.getenv("HOME")))

}

pmml_data=function(pmml_path){
  library("arulesViz")
  fromdisk <- read.PMML(pmml_path) 
}

association_plot_shading= function(rules){
  fromdisk=pmml_data(pmml_path)
  #use ? igraph::layout_ to see layout options with igraph.
  par(mar = c(0, 0, 0, 0) , lwd=1, cex=1)
  plot(head(sort(fromdisk, by="lift"), rules), method="graph", measure = "confidence", shading = "lift", 
       control=list(cex=1, arrowSize=0.6, layout=igraph::nicely(),alpha=0.9))
}
association_plot_itemset= function(rules){
  fromdisk=pmml_data(pmml_path)
  #use ? igraph::layout_ to see layout options with igraph. 
  par(mar = c(0, 0, 0, 0) , lwd=1, cex=1)
  plot(head(sort(fromdisk, by="lift"), rules), method="graph" , 
       control=list(cex=1, arrowSize=0.6,type="itemsets",layout=igraph::nicely(),alpha=0.9))
}

##########################################################################################

bar_plot = function(top_n_apps,column_dis_b, column_dis_v){
  
  data_path = sprintf("%s/distribution.txt", out_dir)
  data = read.table( data_path, sep="\t", header = TRUE, row.names=1)
  m2 = data.matrix(data)
  par(mar=c(9,8,2,0)+0.1, mgp=c(7,1,0))
  data_sub_apps =m2[, order(-colSums(m2))][,1:top_n_apps]
  data_sub_group =data_sub_apps[order(-rowSums(data_sub_apps)), ][1:10,]
  barplot(data_sub_group, cex.names=1, col=brewer.pal(nrow(data_sub_group), "Spectral"), las=2,
          main="Top 10 Executable Usages by Fields of Science",
          ylab="Number of Executions", xlab="Executables")
  legend("topright", legend=gsub("\\*","",rownames(data_sub_group)), fill=brewer.pal(nrow(data_sub_group), "Spectral"), cex = 1,box.col="white", ncol=1, y.intersp=0.8)
  
}
##################################################################################################################
# rules=50
# fromdisk=pmml_data(pmml_path)
# #use ? igraph::layout_ to see layout options with igraph.
# par(mar = c(0, 0, 0, 0) , lwd=0.7, cex=0.8)
# plot(head(sort(fromdisk, by="lift"), rules), method="graph", measure = "confidence", shading = "lift", 
#      control=list(cex=1, arrowSize=0.6, layout=igraph::nicely(),alpha=0.9))
# 
# 
# fromdisk=pmml_data(pmml_path)
# #use ? igraph::layout_ to see layout options with igraph. 
# par(mar = c(0, 0, 0, 0) , lwd=0.7, cex=0.7)
# plot(head(sort(fromdisk, by="lift"), rules), method="graph" , 
#      control=list(cex=1, arrowSize=0.6,type="itemsets",layout=igraph::nicely(),alpha=0.9))
# 


