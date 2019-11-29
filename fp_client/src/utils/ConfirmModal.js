import React from 'react';
import {
    Dialog,
    DialogActions,
    DialogContent,
    Button,
    DialogContentText,
    DialogTitle
} from '@material-ui/core';

const ConfirmModal=({
    open,
    handleConfirm,
    handleClose,
    confirmMessage
}) => {
    return(
        <Dialog 
        open={open} 
        onClose={handleClose}
        aria-labelledby="alert-dialog-title"
        aria-describedby="alert-dialog-description"
         >
            <DialogTitle id="alert-dialog-title">{"Crunchprice"}</DialogTitle>
            <DialogContent>
                <DialogContentText id="alert-dialog-description">
                    {confirmMessage}
                </DialogContentText>
            </DialogContent>
            <DialogActions>

                <Button onClick={handleConfirm} color="primary">
                    확인
                </Button>
                <Button onClick={handleClose} type="submit" color="primary">취소</Button>
            </DialogActions>
        </Dialog>

    )
}
export default ConfirmModal;