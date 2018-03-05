package rpc.test;

public class JobSubmissionProtocolImpl implements JobSummissionProtocol {

	@Override
	public int getProtocolVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Result getHelloWord(boolean say) {
		if(say)
			return new Result("Hello Word");
		else return new Result("not Hello Word");
	}

}
