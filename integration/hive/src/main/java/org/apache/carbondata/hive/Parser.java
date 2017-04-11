package org.apache.carbondata.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 *
 * This class prints out the lineage info. It takes sql as input and prints
 * lineage info. Currently this prints only input and output tables for a given
 * sql. Later we can expand to add join tables etc.
 *
 */
public class Parser implements NodeProcessor {

  TreeSet<String> inputTableList = new TreeSet<String>();
  TreeSet<String> columnList = new TreeSet<String>();

private static Parser parser = new Parser();

  public static Parser getInstance(){
    return parser;
  }
  public TreeSet<String> getInputTableList() {
    return parser.inputTableList;
  }

  public TreeSet<String> getColumnList() {
    return parser.columnList;
  }

  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {
    ASTNode pt = (ASTNode) nd;

    switch (pt.getToken().getType()) {

      case HiveParser.TOK_TABLE_OR_COL:
        parser.columnList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
        break;

      case HiveParser.TOK_TABREF:
        ASTNode tabTree = (ASTNode) pt.getChild(0);
        String table_name = (tabTree.getChildCount() == 1) ?
            BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) :
            BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree
                .getChild(1);
        parser.inputTableList.add(table_name);
        break;
    }
    return null;
  }

  public void getInfo(String query) throws ParseException, SemanticException {

    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(query);

    while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
      tree = (ASTNode) tree.getChild(0);
    }

    parser.inputTableList.clear();
    parser.columnList.clear();
    Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();
    Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(tree);
    ogw.startWalking(topNodes, null);
  }
}