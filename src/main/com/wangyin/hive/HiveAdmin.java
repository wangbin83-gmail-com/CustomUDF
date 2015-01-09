package com.wangyin.hive;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveAdmin extends AbstractSemanticAnalyzerHook {
	private static String admin = "wangke";

	@Override
	public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,
			ASTNode ast) throws SemanticException {
		switch (ast.getToken().getType()) {
			case HiveParser.TOK_CREATEDATABASE:
			case HiveParser.TOK_DROPDATABASE:
			case HiveParser.TOK_CREATEROLE:
			case HiveParser.TOK_DROPROLE:
			case HiveParser.TOK_GRANT:
			case HiveParser.TOK_REVOKE:
			case HiveParser.TOK_GRANT_ROLE:
			case HiveParser.TOK_REVOKE_ROLE:
				String userName = null;
				if (SessionState.get() != null
						&& SessionState.get().getAuthenticator() != null) {
					userName = SessionState.get().getAuthenticator().getUserName();
					if (!admin.equalsIgnoreCase(userName)) {
						throw new SemanticException(userName
								+ " can't use ADMIN options, except " + admin + ".");
					}
					break;
				} else {
					if (SessionState.get() == null) {
						System.out.println("SessionState get is null!");
					}
					if (SessionState.get().getAuthenticator() == null) {
						System.out.println("SessionState getAuthenticator is null!");
					}
				}
			default:
				break;
		}
		return ast;
	}
}