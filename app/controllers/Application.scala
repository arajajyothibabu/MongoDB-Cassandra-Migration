package controllers

import migrations.{ComplexMigration, SimpleMigration}
import play.api._
import play.api.mvc._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class Application extends Controller {

  def index = Action {
    Future{
      ComplexMigration.main(new Array[String](0))
    }
    Ok(views.html.index("Check the Console..!"))
  }

}