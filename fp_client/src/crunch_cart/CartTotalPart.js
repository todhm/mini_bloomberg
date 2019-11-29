import React, {Component, Fragment} from 'react';
import {styles} from '../utils/styles'
import {Grid,Card,CardContent} from '@material-ui/core';
import {withStyles} from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';

const CartTotalPart=({title,sumText,additional,classes})=>{
    return(
        <Card >
            <CardContent>
                    <Typography >
                        {title}
                    </Typography>
                    <Typography>
                        {sumText}
                    </Typography>
                    <Typography className={classes.pos} color="textSecondary">
                        {additional}
                    </Typography>
            </CardContent>
        </Card>
    )
}



export default withStyles(styles)(CartTotalPart);