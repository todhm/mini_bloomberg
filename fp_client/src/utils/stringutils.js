export const getMoneyString=(price)=>{
    return price.toString().split( /(?=(?:...)*$)/ ).join(',')+"원";

}