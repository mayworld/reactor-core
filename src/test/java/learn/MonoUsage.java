package learn;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class MonoUsage {

  public Disposable testRepeat(){
    return Mono.fromCallable(()->System.currentTimeMillis())
        .repeat()
//        .publishOn(Schedulers.single())
//        .log("foo.bar")
        .subscribe(System.out::println);
  }

  @Test
  public void testWithWarm(){
    Disposable justUseAsWarm = testRepeat();

    justUseAsWarm.dispose();
    if(justUseAsWarm.isDisposed()){
      testRepeat();
    }
  }

  @Test
  public void testSequence(){
    Mono.just("may")
        .publishOn(Schedulers.newElastic("STAGE-1"))
        .then(new NewMonoFunction<>("meet beiLei"))
        .publishOn(Schedulers.newElastic("STAGE-2"))
        .then(new NewMonoFunction<>("we chat"))
        .publishOn(Schedulers.newElastic("STAGE-3"))
        .then(new NewMonoFunction<>("meet again "))
        .publishOn(Schedulers.newElastic("STAGE-4"))
        .then(new NewMonoFunction<>("family travel "))
        .publishOn(Schedulers.newElastic("STAGE-5"))
        .then(new NewMonoFunction<>("know each other"))
        .publishOn(Schedulers.newElastic("STAGE-6"))
        .concatWith(Mono.just("beiLei "))
        .publishOn(Schedulers.newElastic("STAGE-N"))
        .subscribe(new Subscriber<String>() {
          private int received =1;
          private StringBuilder message=new StringBuilder();
          private Subscription subscription;
          @Override
          public void onSubscribe(Subscription s) {
            System.out.println(Thread.currentThread()+" in ");
            this.subscription = s;
            s.request(1);
          }

          @Override
          public void onNext(String s) {
            if(received==1) {
              message.append(s);
            }else{
              message.append(" and ").append(s);
            }
            received++;
            subscription.request(1);
          }

          @Override
          public void onError(Throwable t) {

          }

          @Override
          public void onComplete() {
            System.out.println(Thread.currentThread()+" "+received+" is not end");
          }
        });
    try {
      Thread.sleep(1234L);
    } catch (InterruptedException e){
      e.printStackTrace();
    }
  }

  public static <Pre,Next> void printlnThread(Pre preValue,Next nextValue){
    System.out.println(Thread.currentThread()+" : preValue : "+preValue+" nextValue :"+nextValue);
  }

  public static class NewMonoFunction<In,Out> implements Function<In,Mono<Out>> {

    private Out outValue;

    public NewMonoFunction(Out outValue){
      this.outValue =outValue;
    }
    @Override
    public Mono<Out> apply(In in)  {
      printlnThread(in,outValue);
      return Mono.just(outValue);
    }
  }
}
