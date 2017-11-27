package mimir.exec.mode

class JoinInfo(val table1:String,val table2:String,val col1:String,val col2:String,val alias1:String,val alias2:String, val det1:Boolean ,val det2:Boolean)
{
  def getTable1 = table1
  def getTable2 = table2
  def getCol1 = col1
  def getCol2 = col2
  def getNonDet1= det1
  def getNonDet2 = det2
}
