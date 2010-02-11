<?php 
# 
# xmlrpc-socket - PHP XML-RPC gateway
# beta version 1
# 
# Copyright 2001 Scott Andrew LePera
# scott@scottandrew.com
# http://www.scottandrew.com/xml-rpc
# 
# License: 
# You are granted the right to use and/or redistribute this 
# code only if this license and the copyright notice are included 
# and you accept that no warranty of any kind is made or implied 
# by the author. 
#
# Requires Brent Ashley's JSRS Server PHP component.  Download
# the latest version from http://www.ashleyit.com/rs

require("http_post.util");
require("jsrsServer.php.inc");

jsrsDispatch("doRPC");

function doRPC($rpcserver,$rpctext){
  if (!isset($rpcserver)){
    jsrsReturnError("No rpcserver specified.");
    die;
  }
  if (!isset($rpctext)){
    jsrsReturnError("No rpctext specified.");
    die;
  }
  $rpctext=urldecode($rpctext);
  $rpctext=str_replace("\n","",$rpctext);
  $rpctext=str_replace("\r","",$rpctext);
  $a=new http_post;
  $a->set_action($rpcserver);
  $a->set_enctype("text/xml");
  $a->set_useragent("JavaScript/PHP XMLRPC Client/1.0");
  $a->set_body($rpctext);
  $response = $a->send();
  $startXML =  strpos ($response, "<?");
  $methodResponse =  substr($response,$startXML);
  jsrsReturn($methodResponse);
}
?>
