/* Generated By:JavaCC: Do not edit this line. MimirJSqlParserConstants.java */
/* ================================================================
 * MimirJSqlParser : java based sql parser 
 *
 * Forked from JSQLParser 
 *   by Leonardo Francalanci (leoonardoo@yahoo.it)
 *   info at: http://jsqlparser.sourceforge.net
 *
 * ************************ IMPORTANT *****************************
 * This file (MimirJsqlParser.java) is AUTOGENERATED from 
 * JSqlParserCC.jj
 * 
 * DO NOT EDIT MimirJsqlParser.java DIRECTLY!!!
 * 
 * Instead, edit JSqlParserCC.jj and use `sbt parser` to rebuild.
 * ================================================================
 */


package mimir.parser;


/**
 * Token literal values and constants.
 * Generated by org.javacc.parser.OtherFilesGen#start()
 */
public interface MimirJSqlParserConstants {

  /** End of File. */
  int EOF = 0;
  /** RegularExpression Id. */
  int K_AS = 5;
  /** RegularExpression Id. */
  int K_DIRECT = 6;
  /** RegularExpression Id. */
  int K_UNCERTAIN = 7;
  /** RegularExpression Id. */
  int K_ANALYZE = 8;
  /** RegularExpression Id. */
  int K_EXPLAIN = 9;
  /** RegularExpression Id. */
  int K_ASSUME = 10;
  /** RegularExpression Id. */
  int K_VIEW = 11;
  /** RegularExpression Id. */
  int K_LENS = 12;
  /** RegularExpression Id. */
  int K_ADAPTIVE = 13;
  /** RegularExpression Id. */
  int K_SCHEMA = 14;
  /** RegularExpression Id. */
  int K_LET = 15;
  /** RegularExpression Id. */
  int K_LOAD = 16;
  /** RegularExpression Id. */
  int K_PLOT = 17;
  /** RegularExpression Id. */
  int K_ALTER = 18;
  /** RegularExpression Id. */
  int K_SAVE = 19;
  /** RegularExpression Id. */
  int K_RENAME = 20;
  /** RegularExpression Id. */
  int K_PRAGMA = 21;
  /** RegularExpression Id. */
  int K_MATERIALIZE = 22;
  /** RegularExpression Id. */
  int K_BY = 23;
  /** RegularExpression Id. */
  int K_DO = 24;
  /** RegularExpression Id. */
  int K_IF = 25;
  /** RegularExpression Id. */
  int K_IS = 26;
  /** RegularExpression Id. */
  int K_IN = 27;
  /** RegularExpression Id. */
  int K_OR = 28;
  /** RegularExpression Id. */
  int K_OF = 29;
  /** RegularExpression Id. */
  int K_ON = 30;
  /** RegularExpression Id. */
  int K_ALL = 31;
  /** RegularExpression Id. */
  int K_AND = 32;
  /** RegularExpression Id. */
  int K_ANY = 33;
  /** RegularExpression Id. */
  int K_KEY = 34;
  /** RegularExpression Id. */
  int K_NOT = 35;
  /** RegularExpression Id. */
  int K_SET = 36;
  /** RegularExpression Id. */
  int K_ASC = 37;
  /** RegularExpression Id. */
  int K_TOP = 38;
  /** RegularExpression Id. */
  int K_END = 39;
  /** RegularExpression Id. */
  int K_DESC = 40;
  /** RegularExpression Id. */
  int K_INTO = 41;
  /** RegularExpression Id. */
  int K_NULL = 42;
  /** RegularExpression Id. */
  int K_LIKE = 43;
  /** RegularExpression Id. */
  int K_DROP = 44;
  /** RegularExpression Id. */
  int K_JOIN = 45;
  /** RegularExpression Id. */
  int K_LEFT = 46;
  /** RegularExpression Id. */
  int K_FROM = 47;
  /** RegularExpression Id. */
  int K_OPEN = 48;
  /** RegularExpression Id. */
  int K_CASE = 49;
  /** RegularExpression Id. */
  int K_WHEN = 50;
  /** RegularExpression Id. */
  int K_THEN = 51;
  /** RegularExpression Id. */
  int K_ELSE = 52;
  /** RegularExpression Id. */
  int K_SOME = 53;
  /** RegularExpression Id. */
  int K_FULL = 54;
  /** RegularExpression Id. */
  int K_WITH = 55;
  /** RegularExpression Id. */
  int K_TABLE = 56;
  /** RegularExpression Id. */
  int K_WHERE = 57;
  /** RegularExpression Id. */
  int K_USING = 58;
  /** RegularExpression Id. */
  int K_UNION = 59;
  /** RegularExpression Id. */
  int K_GROUP = 60;
  /** RegularExpression Id. */
  int K_BEGIN = 61;
  /** RegularExpression Id. */
  int K_INDEX = 62;
  /** RegularExpression Id. */
  int K_INNER = 63;
  /** RegularExpression Id. */
  int K_LIMIT = 64;
  /** RegularExpression Id. */
  int K_OUTER = 65;
  /** RegularExpression Id. */
  int K_ORDER = 66;
  /** RegularExpression Id. */
  int K_RIGHT = 67;
  /** RegularExpression Id. */
  int K_DELETE = 68;
  /** RegularExpression Id. */
  int K_CREATE = 69;
  /** RegularExpression Id. */
  int K_SELECT = 70;
  /** RegularExpression Id. */
  int K_CAST = 71;
  /** RegularExpression Id. */
  int K_PROVENANCE = 72;
  /** RegularExpression Id. */
  int K_OFFSET = 73;
  /** RegularExpression Id. */
  int K_EXISTS = 74;
  /** RegularExpression Id. */
  int K_HAVING = 75;
  /** RegularExpression Id. */
  int K_INSERT = 76;
  /** RegularExpression Id. */
  int K_UPDATE = 77;
  /** RegularExpression Id. */
  int K_VALUES = 78;
  /** RegularExpression Id. */
  int K_ESCAPE = 79;
  /** RegularExpression Id. */
  int K_PRIMARY = 80;
  /** RegularExpression Id. */
  int K_NATURAL = 81;
  /** RegularExpression Id. */
  int K_REPLACE = 82;
  /** RegularExpression Id. */
  int K_BETWEEN = 83;
  /** RegularExpression Id. */
  int K_TRUNCATE = 84;
  /** RegularExpression Id. */
  int K_DISTINCT = 85;
  /** RegularExpression Id. */
  int K_INTERSECT = 86;
  /** RegularExpression Id. */
  int K_FEEDBACK = 87;
  /** RegularExpression Id. */
  int K_EXTRACT = 88;
  /** RegularExpression Id. */
  int S_DOUBLE = 89;
  /** RegularExpression Id. */
  int S_INTEGER = 90;
  /** RegularExpression Id. */
  int DIGIT = 91;
  /** RegularExpression Id. */
  int LINE_COMMENT = 92;
  /** RegularExpression Id. */
  int MULTI_LINE_COMMENT = 93;
  /** RegularExpression Id. */
  int S_IDENTIFIER = 94;
  /** RegularExpression Id. */
  int LETTER = 95;
  /** RegularExpression Id. */
  int SPECIAL_CHARS = 96;
  /** RegularExpression Id. */
  int S_CHAR_LITERAL = 97;
  /** RegularExpression Id. */
  int S_QUOTED_IDENTIFIER = 98;

  /** Lexical state. */
  int DEFAULT = 0;

  /** Literal token values. */
  String[] tokenImage = {
    "<EOF>",
    "\" \"",
    "\"\\t\"",
    "\"\\r\"",
    "\"\\n\"",
    "\"AS\"",
    "\"DIRECT\"",
    "\"UNCERTAIN\"",
    "\"ANALYZE\"",
    "\"EXPLAIN\"",
    "\"ASSUME\"",
    "\"VIEW\"",
    "\"LENS\"",
    "\"ADAPTIVE\"",
    "\"SCHEMA\"",
    "\"LET\"",
    "\"LOAD\"",
    "\"PLOT\"",
    "\"ALTER\"",
    "\"SAVE\"",
    "\"RENAME\"",
    "\"PRAGMA\"",
    "\"MATERIALIZE\"",
    "\"BY\"",
    "\"DO\"",
    "\"IF\"",
    "\"IS\"",
    "\"IN\"",
    "\"OR\"",
    "\"OF\"",
    "\"ON\"",
    "\"ALL\"",
    "\"AND\"",
    "\"ANY\"",
    "\"KEY\"",
    "\"NOT\"",
    "\"SET\"",
    "\"ASC\"",
    "\"TOP\"",
    "\"END\"",
    "\"DESC\"",
    "\"INTO\"",
    "\"NULL\"",
    "\"LIKE\"",
    "\"DROP\"",
    "\"JOIN\"",
    "\"LEFT\"",
    "\"FROM\"",
    "\"OPEN\"",
    "\"CASE\"",
    "\"WHEN\"",
    "\"THEN\"",
    "\"ELSE\"",
    "\"SOME\"",
    "\"FULL\"",
    "\"WITH\"",
    "\"TABLE\"",
    "\"WHERE\"",
    "\"USING\"",
    "\"UNION\"",
    "\"GROUP\"",
    "\"BEGIN\"",
    "\"INDEX\"",
    "\"INNER\"",
    "\"LIMIT\"",
    "\"OUTER\"",
    "\"ORDER\"",
    "\"RIGHT\"",
    "\"DELETE\"",
    "\"CREATE\"",
    "\"SELECT\"",
    "\"CAST\"",
    "\"PROVENANCE\"",
    "\"OFFSET\"",
    "\"EXISTS\"",
    "\"HAVING\"",
    "\"INSERT\"",
    "\"UPDATE\"",
    "\"VALUES\"",
    "\"ESCAPE\"",
    "\"PRIMARY\"",
    "\"NATURAL\"",
    "\"REPLACE\"",
    "\"BETWEEN\"",
    "\"TRUNCATE\"",
    "\"DISTINCT\"",
    "\"INTERSECT\"",
    "\"FEEDBACK\"",
    "\"EXTRACT\"",
    "<S_DOUBLE>",
    "<S_INTEGER>",
    "<DIGIT>",
    "<LINE_COMMENT>",
    "<MULTI_LINE_COMMENT>",
    "<S_IDENTIFIER>",
    "<LETTER>",
    "<SPECIAL_CHARS>",
    "<S_CHAR_LITERAL>",
    "<S_QUOTED_IDENTIFIER>",
    "\";\"",
    "\":\"",
    "\"(\"",
    "\",\"",
    "\")\"",
    "\"=\"",
    "\".\"",
    "\"*\"",
    "\"?\"",
    "\">\"",
    "\"<\"",
    "\">=\"",
    "\"<=\"",
    "\"<>\"",
    "\"!=\"",
    "\"@@\"",
    "\"||\"",
    "\"|\"",
    "\"&\"",
    "\"+\"",
    "\"-\"",
    "\"/\"",
    "\"^\"",
    "\"{d\"",
    "\"}\"",
    "\"{t\"",
    "\"{ts\"",
    "\"{fn\"",
  };

}
