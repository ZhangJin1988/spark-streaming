package scalikejdbc

import scalikejdbc.config.DBs

/**
  * Author: sheep.Old  
  * WeChat: JiaWei-YANG
  * QQ: 64341393 
  * Created 2018/6/20
  */

case class Users(id: Int, name: String, nickName: String)

object TestScalikejdbc {

    DBs.setup() // application.conf ->application.json -> application.properties

    // 插入
    def testInsert() = {
        DB.autoCommit { implicit session => SQL("insert into users(name, nickname) values(?,?)").bind("zhaomin", "minmin").update().apply() }
    }

    // 删除
    def testDelete = {
        DB.autoCommit { implicit session =>
            SQL("delete from users where name=?").bind("zhaomin").update().apply()
        }
    }

    def testUpdate = {
        DB.autoCommit { implicit session =>
            SQL("update users set name='小虎子' where id=28").update().apply()
        }
    }

    def testSelect = {
        DB.readOnly{ implicit session =>
            SQL("select * from users").map(rs => {
                Users(
                    rs.int("id"),
                    rs.string("name"),
                    rs.string("nickname")
                )
            }).list().apply()
        }
    }

    def testTransaction = {
        DB.localTx{ implicit session =>
            SQL("insert into users(name, nickname) values(?,?)").bind("xiaoqian", "qianqian").update().apply()

            // val i = 1/0

            SQL("insert into users(name, nickname) values(?,?)").bind("heishanlaoyao", "laoyao").update().apply()
        }
    }


    def main(args: Array[String]): Unit = {

//        testInsert()
//        testDelete
//        testUpdate

//        val userList = testSelect
//        userList.foreach(println)

        testTransaction
    }

}
