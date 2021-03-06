package com.interactcrm.qstats.servlets;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.interactcrm.logging.Log;
import com.interactcrm.logging.factory.LogModuleFactory;
import com.interactcrm.qstats.startup.AgentMonitorStore;
import com.interactcrm.util.logging.LogHelper;

/**
 * Servlet implementation class GetMyAgentData
 */

public class GetMyAgentData extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private Log _logger = new LogHelper(GetData.class).getLogger(LogModuleFactory.getModule("QueueStatistics"));
    private boolean _debugLog = false;
    private boolean _errorLog = false;
    private boolean _infoLog = false;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public GetMyAgentData() {
        super();
        // TODO Auto-generated constructor stub
    }

	public void init(ServletConfig config) throws ServletException {
		if(_logger != null){
    		_debugLog = _logger.isDebugEnabled();
    		_errorLog =  _logger.isErrorEnabled();
    		_infoLog  = _logger.isInfoEnabled();
    	}
	}
	
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost( request , response );
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
		int tenantGrpId	=	Integer.parseInt(request.getParameter("tenangGrpPKey"));
		if(_infoLog){
			_logger.info(" [GetMyAgentData] call servlet :: /GetMyAgentData\n Request parameter fetched is :: "+tenantGrpId);
		}
		response.setContentType("text/html");
		PrintWriter	out	=response.getWriter();
		
		String responseFrmQM	=	AgentMonitorStore.getInstance().getAgentMonitoreData(tenantGrpId);
		if(_infoLog){
			_logger.info(" [GetMyAgentData] sending response to AD "+responseFrmQM);
		}
		out.println(responseFrmQM);
	}

}
