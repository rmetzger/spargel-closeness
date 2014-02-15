package de.robertmetzger;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.nephele.client.JobExecutionResult;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.spargel.java.SpargelIteration;
import eu.stratosphere.types.LongValue;

public class Closeness implements Program {


	public static void main(String[] args) throws Exception {
		// at home
//		String[] myArgs = {"1", 
//		"file:///home/robert/Projekte/Studium/TUBerlin/Semester2/AIM3/project/data/small.txt",
//		"file:///home/robert/Projekte/Studium/TUBerlin/Semester3/IMPRO3/spargel-closeness/stratoOut",
//		"100"};
//		String[] myArgs = {"1", 
//		"file:///home/robert/Projekte/Studium/TUBerlin/Semester3/IMPRO3/spargel-closeness/enron-clean2.txt",
//		"file:///home/robert/Projekte/Studium/TUBerlin/Semester3/IMPRO3/spargel-closeness/enronOut",
//		"100"};
		
//		
//		// laptop
//		String[] myArgs = {"1", 
//		"file:///home/robert/Projekte/ozone/spargel-closeness/enron-clean2.txt",
//		"file:///home/robert/Projekte/ozone/spargel-closeness/enronout",
//		"100"};
		
		String[] myArgs = {"1", 
			"file:///home/robert/Projekte/ozone/spargel-closeness/test2",
			"file:///home/robert/Projekte/ozone/spargel-closeness/testout",
			"100", "\\t", "\\t"};
		JobExecutionResult res = LocalExecutor.execute(new Closeness(), myArgs);
		System.err.println("Accu Res "+AccumulatorHelper.getResultsFormated(res.getAllAccumulatorResults()));
	}
	public static class ArgsParser {
		private String[] args;
		int pos = 0;

		public ArgsParser(String... args) {
			this.args = args;
		}
		private void check() {
			if(pos >= args.length) {
				throw new RuntimeException("There are only "+args.length+" available");
			}
		}
		
		/**
		 * This actually only works for strings.
		 * @return
		 */
		public <T> T get() {
			return (T) args[pos++];
		}
		
		public int i() {
			check();
			return Integer.parseInt(args[pos++]);
		}
		
		public int i(int def) {
			if(pos < args.length) {
				return Integer.parseInt(args[pos++]);
			} else {
				return def;
			}
		}
		public String s() {
			check();
			return args[pos++];
		}
		
		public String s(String def) {
			if(pos < args.length) {
				return args[pos++];
			} else {
				return def;
			}
		}
	}

	@Override
	public Plan getPlan(String... args) {
		ArgsParser p = new ArgsParser(args);
		final String mode = p.get();
		final int dop = p.i(1);
		final String inputPath = p.s();
		final String resultPath = p.s();
		final int maxIterations = p.i(10);
		final String fromSplit = p.s("\\t");
		final String toSplit = p.s(",");
		System.err.println("Using fromSplit='"+fromSplit+"', toSplit='"+toSplit+"'");
		
		FileDataSource input = new FileDataSource(new TextInputFormat(), inputPath, "Input");
		MapOperator edges = MapOperator .builder(new LinesToEdges(fromSplit, toSplit))
										.input(input)
										.build();
		
		MapOperator initialVertices = MapOperator.builder(InitializeVertices.class)
										.input(input).build();
		// initialVertices.setParameter(TaskConfig.DEFAULT_ACCUMULATORS, true);
		
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
