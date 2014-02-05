package de.robertmetzger;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.spargel.java.SpargelIteration;
import eu.stratosphere.types.LongValue;

public class Closeness implements Program {


	public static void main(String[] args) throws Exception {
		// at home
//		String[] myArgs = {"1", 
//		"file:///home/robert/Projekte/Studium/TUBerlin/Semester2/AIM3/project/data/small.txt",
//		"file:///home/robert/Projekte/Studium/TUBerlin/Semester3/IMPRO3/spargel-closeness/stratoOut",
//		"100"};
		String[] myArgs = {"1", 
		"file:///home/robert/Projekte/Studium/TUBerlin/Semester3/IMPRO3/spargel-closeness/enron-clean2.txt",
		"file:///home/robert/Projekte/Studium/TUBerlin/Semester3/IMPRO3/spargel-closeness/enronOut",
		"100"};
		
//		
		// laptop
//		String[] myArgs = {"1", 
//		"file:///home/robert/Projekte/ozone/spargel-closeness/enron-clean2.txt",
//		"file:///home/robert/Projekte/ozone/spargel-closeness/enronout",
//		"100"};
		
//		String[] myArgs = {"1", 
//			"file:///home/robert/Projekte/ozone/spargel-closeness/test.txt",
//			"file:///home/robert/Projekte/ozone/spargel-closeness/testout",
//			"100"};
		LocalExecutor.execute(new Closeness(), myArgs);
	}

	@Override
	public Plan getPlan(String... args) {
		final int dop = args.length > 0 ? Integer.parseInt(args[0]) : 1;
		final String inputPath = args.length > 1 ? args[1] : "";
		final String resultPath = args.length > 2 ? args[2] : "";
		final int maxIterations = args.length > 3 ? Integer.parseInt(args[3]) : 10;
		
		FileDataSource input = new FileDataSource(new TextInputFormat(), inputPath, "Input");
		MapOperator edges = MapOperator .builder(LinesToEdges.class)
										.input(input)
										.build();
		
		MapOperator initialVertices = MapOperator.builder(InitializeVertices.class)
										.input(input).build();
		
		SpargelIteration iteration = new SpargelIteration(
			new HLLMessager(), new HLLVertex(), "HyperLogLog Closeness (Spargel API)");
		iteration.setVertexInput(initialVertices);
		iteration.setEdgesInput(edges);
		iteration.setNumberOfIterations(maxIterations);
		
		// create DataSinkContract for writing the new cluster positions
		FileDataSink result = new FileDataSink(CsvOutputFormat.class, resultPath, "Result");
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(LongValue.class, 0)
			.field(VertexValue.class, 1);
		result.setInput(iteration.getOutput());

		Plan p = new Plan(result);
		p.setDefaultParallelism(dop);
		return p;
	}

}
