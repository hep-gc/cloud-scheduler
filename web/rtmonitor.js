<!--

function pollRPCServer(target)
{
   var result = document.getElementById(target).innerHTML;
   document.getElementById(target).innerHTML = result;
   myRPCall("get_json_resource");
   setTimeout("pollRPCServer('main_info')", 10000);
}

function buildTable(resource_pool)
{
   var table = '<table border=1 cellpadding=5 width="50%">';
   var header = "<tr><th>Cluster</th><th>Virtual Machines</th><th>Status</th></tr>";
   table = table + header;
   for(var i = 0; i < resource_pool.resources.length; i = i +1)
   {
      var spanrows = resource_pool.resources[i].vms.length;
      if(spanrows == 0) spanrows = 1;
      var row = '<tr> <td align="left" rowspan='+spanrows+"><p>Name:  "+resource_pool.resources[i].name+"</p><p>Type:  "+resource_pool.resources[i].cloud_type+"</p><p>Available VM Slots:  "+resource_pool.resources[i].vm_slots +"</p></td>";
      table = table + row;
      if(resource_pool.resources[i].vms.length == 0)
         table = table + "</tr>";
      for(var j = 0; j < resource_pool.resources[i].vms.length; j = j + 1)
      {
         var vrow = "";
         if(j >=1) vrow = "<tr>";
         var stat = resource_pool.resources[i].vms[j].status;
         var bgc = '"#FFFFFF"';
         if(stat == "Running")
            bgc = '"#00FF00"';
         else if(stat == "Starting")
            bgc = '"#FFFF00"';
         else
            bgc = '"#FF0000"';
         vrow = vrow + '<td align="left"><p>'+resource_pool.resources[i].vms[j].name +"</p>mem: "+resource_pool.resources[i].vms[j].memory+"</p><p>cpus: "+resource_pool.resources[i].vms[j].cpucores+"</p><p>type: "+resource_pool.resources[i].vms[j].vmtype+'</p></td><td align="center" bgcolor='+bgc+">"+stat+"</td></tr>";
         table = table + vrow;
      }
   }
   table = table + "</table>";
   document.getElementById('tb').innerHTML = table;
}

function myCallback(response)
{
   //document.getElementById('head_info').innerHTML = response + "<br/>Length: "+response.length;
   var pattern = /[{].*.[}]/
   var json = pattern.exec(response);
   if(json != null)
   {
      document.getElementById('main_info').innerHTML = json[0] + "<br/>Length: "+json[0].length + " num: "+json.length;
      var js = json[0];
      var resource = json_parse(js); 
      document.getElementById('tb').innerHTML = "updating";
      buildTable(resource);
   }
   else
   {
      document.getElementById('tb').innerHTML = "<p>Error Reading from RPC Server</p>";
   }
}

function myRPCall(method)
{
   var msg = new XMLRPCMessage();
   msg.setMethod(method);
   //msg.addParameter("vmcgs29");
   var msgtext = msg.xml();

   //alert(msgtext);
   var msgs = [];
   msgs[0] = "http://vmcgs29.phys.uvic.ca:8111/";
   msgs[1] = escape(msgtext);
   jsrsExecute("xmlrpc-socket.php", myCallback, "doRPC", msgs);

}

//--!>

