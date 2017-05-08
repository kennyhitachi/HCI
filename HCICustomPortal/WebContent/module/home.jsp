<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html>
<!--[if lt IE 7]> <html class="lt-ie9 lt-ie8 lt-ie7" lang="en"> <![endif]-->
<!--[if IE 7]> <html class="lt-ie9 lt-ie8" lang="en"> <![endif]-->
<!--[if IE 8]> <html class="lt-ie9" lang="en"> <![endif]-->
<!--[if gt IE 8]><!--> <html lang="en"> <!--<![endif]-->
<head>
  <meta charset="utf-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1">
  <title>Home</title>
</head>
<body>
<p>Welcome : <%=session.getAttribute("UserName") %><p>
   <div>
	<table style="width:10px"><tr><td><section class="container well">
		  <select>
  <option value="User">User</option>
  <option value="Last Modified Time">Last Modified Time</option>
  <option value="Creation Time">Creation Time</option>
  <option value="Choose Fields" selected>Choose Fields</option>
</select>
	</section></td><td><section class="container well">
	  <select>
	    <option value="operators" selected>operators</option>
	    <option value="is">is</option>
	    <option value="is greater than">is greater than</option>
	    <option value="is less than">is less than</option>
	    <option value="contains">contains</option>
	    <option value="exists">exists</option>
	  </select>
	</section></td><td><input type="text"></td></tr>
	</table>
	<button >Search</button>
  <h2></h2>
</div>
</body>
</html>
