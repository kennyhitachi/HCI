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
  <title>Login Form</title>
 <!-- <link rel="stylesheet" href="css/style.css"> -->
 <style>
div.relative {
    width: 300px;
    height: 100px;
    position: absolute;
    top:0;
    bottom: 200px;
    left: 0;
    right: 0;

    margin: auto;
}
</style>
</head>
<body>
    <div class="relative">
      <h1>HCI Search Login</h1>
      <form method="post" action="Login.do">
        <p><input type="text" name="login" value="" placeholder="Username"></p>
        <p><input type="password" name="password" value="" placeholder="Password"></p>
        Realm:
        <select>
	       <option value="Local" selected>Local</option>
	    </select>
        <p><input type="submit" name="commit" value="Login"></p>
      </form>
    </div>
</body>
</html>
