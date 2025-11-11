public class WordCountTop3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> source = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Long>> counts = source
                .flatMap((String line, Collector<String> out) -> {
                    for (String w : line.split("\\s")) out.collect(w);
                })
                .returns(Types.STRING)
                .map(w -> Tuple2.of(w, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1));

        // 窗口内 TopN
        DataStream<String> top3 = counts
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new TopNAllFunction(3));

        top3.print();
        env.execute();
    }

    public static class TopNAllFunction extends ProcessAllWindowFunction<
            Tuple2<String, Long>, String, TimeWindow> {
        private final int n;
        public TopNAllFunction(int n) { this.n = n; }
        @Override
        public void process(Context ctx, Iterable<Tuple2<String, Long>> iter,
                            Collector<String> out) {
            List<Tuple2<String, Long>> list = new ArrayList<>();
            iter.forEach(list::add);
            list.sort((a, b) -> Long.compare(b.f1, a.f1));
            StringBuilder sb = new StringBuilder("Top" + n + " @ " + ctx.window().getEnd() + ": ");
            for (int i = 0; i < Math.min(n, list.size()); i++) {
                sb.append(list.get(i).f0).append("=").append(list.get(i).f1).append(" ");
            }
            out.collect(sb.toString());
        }
    }
}
