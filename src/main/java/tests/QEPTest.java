package tests;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.HashJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;

public class QEPTest extends TestDriver {

  protected static final String TEST_NAME = "query evaluation pipeline tests";

  protected static Schema s_employee;
  protected static Schema s_department;

  public static void main(String[] args) {

    QEPTest qept = new QEPTest();
    qept.create_minibase();

    s_employee = new Schema(5);
    s_employee.initField(0, AttrType.INTEGER, 4, "EmpId");
    s_employee.initField(1, AttrType.STRING, 20, "Name");
    s_employee.initField(2, AttrType.INTEGER, 4, "Age");
    s_employee.initField(3, AttrType.INTEGER, 4, "Salary");
    s_employee.initField(4, AttrType.INTEGER, 4, "DeptId");

    s_department = new Schema(4);
    s_department.initField(0, AttrType.INTEGER, 4, "DeptId");
    s_department.initField(1, AttrType.STRING, 50, "Name");
    s_department.initField(2, AttrType.INTEGER, 4, "MinSalary");
    s_department.initField(3, AttrType.INTEGER, 4, "MaxSalary");

    System.out.println("\n" + "Running " + TEST_NAME + "...");
    boolean status = PASS;
    status &= qept.test1();
    status &= qept.test2();
    status &= qept.test3();
    status &= qept.test4();

    // display the final results
    System.out.println();
    if (status != PASS) {
        System.out.println("Error(s) encountered during " + TEST_NAME + ".");
        System.exit(-1);
    } else {
        System.out.println("All " + TEST_NAME
                + " completed; verify output for correctness.");
    }
  }

  // Display for each employee his ID, Name and Age
  protected boolean test1() {
    // ignore
    return true;
  }

  // Display the Name for the departments with MinSalary = MaxSalary
  protected boolean test2() {
    // ignore
    return true;
  }

  // For each employee, display his Name and the Name of his 
  //  department as well as the maximum salary of his department
  protected boolean test3() {
    // ignore
    return true;
  }

  // Display the Name for each employee whose Salary is
  //  greater than the maximum salary of his department.
  protected boolean test4() {
    // ignore
    return true;
  }
}
