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

import java.io.File;

public class QEPTest extends TestDriver {

  protected static final String TEST_NAME = "query evaluation pipeline tests";

  protected static Schema s_employee;
  protected static Schema s_department;

  public static void main(String[] args) {

    File emps_file;
    File dept_file;

    if (args.length > 0) {
      String rel_path = args[0];
      if (rel_path.charAt(rel_path.length() - 1) != '/') {
        rel_path = rel_path.concat("/");
      }
      System.out.println(rel_path);
      emps_file = new File(rel_path.concat("Employee.txt"));
      dept_file = new File(rel_path.concat("Department.txt"));
    } else {
      // init a default location for the files here
    }

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
    
    System.out.println("\nTest 1: Display for each employee his ID, Name and Age\n");

    try {
      this.initCounts();
      this.saveCounts(null);

      HeapFile employees = new HeapFile(null);
      Tuple tuple = new Tuple(s_employee);
      tuple.setAllFields(1, "Nick", 25, 900, 1);
      tuple.insertIntoFile(employees);
      tuple.setAllFields(6, "John", 40, 15000, 5);
      tuple.insertIntoFile(employees);
      tuple.setAllFields(7, "Josef", 32, 7000, 1);
      tuple.insertIntoFile(employees);
      this.saveCounts("emps");

      this.saveCounts(null);
      FileScan scan = new FileScan(s_employee, employees);

      Projection pro = new Projection(scan, 0, 1, 2);
      pro.explain(0);
      System.out.println();
      pro.execute();
      this.saveCounts("project");

      System.out.print("\n\nTest 1 completed without exception.");

      return PASS;
    } catch (Exception e) {
      e.printStackTrace(System.out);
      System.out.print("\n\nTest 1 terminated because of exception.");
      return FAIL;
    } finally {
      printSummary(2);
      System.out.println();
    }
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
