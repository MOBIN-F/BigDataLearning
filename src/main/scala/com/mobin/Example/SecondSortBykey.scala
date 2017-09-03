package com.mobin.Example

/**
  * Created by Mobin on 2017/9/3.
  */
class SecondSortBykey(val first: Int, val second: Int) extends Ordered [SecondSortBykey] with Serializable {
  def compare(other:SecondSortBykey):Int = {
    if (this.first - other.first !=0) {
      this.first - other.first
    } else {
      this.second - other.second
    }
  }
}
