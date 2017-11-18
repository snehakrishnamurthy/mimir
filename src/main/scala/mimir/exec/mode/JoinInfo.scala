package mimir.exec.mode

class JoinInfo(val table1:String,val table2:String,val col1:String,val col2:String)
{
  def getTable1 = table1
  def getTable2 = table2
  def getCol1 = col1
  def getCol2 = col2
}
