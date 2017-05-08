package com.hds.custom.servlets;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;


import com.hds.custom.hci.HCILogin;

/**
 * Servlet implementation class LoginServlet
 */
@WebServlet("/Login.do")
public class LoginServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public LoginServlet() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		
		response.setContentType("text/html");  
		PrintWriter out = response.getWriter();  
		
		String userName = request.getParameter("login");
		String userPass = request.getParameter("password");

		request.setAttribute("UserName", userName);
		request.setAttribute("Password", userPass);

		HttpSession session = request.getSession();

		HCILogin hciLogin = new HCILogin(userName, userPass);
		
		try {
			hciLogin.initialize();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String authToken = hciLogin.login();

		session.setAttribute("UserName", userName);
		if (authToken.length() > 0) {
			session.setAttribute("AuthToken", authToken);
			RequestDispatcher rd=request.getRequestDispatcher("module/home.jsp");  
	        rd.forward(request, response); 
			
		} else {
			 out.print("<p><font color=\"red\">Invalid credentials. Please try again!</font></p>");  
		     RequestDispatcher rd=request.getRequestDispatcher("/Login.jsp");  
		     rd.include(request, response);  
			 session.setAttribute("AuthToken", null);
			
		}

	}

}
