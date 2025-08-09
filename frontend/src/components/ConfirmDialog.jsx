import * as Dialog from '@radix-ui/react-dialog'
import React from 'react'

export default function ConfirmDialog({open, onOpenChange, title='Confirm', description='Are you sure?', onConfirm}){
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 bg-black/40" />
        <Dialog.Content className="fixed left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 card w-[90vw] max-w-md">
          <Dialog.Title className="text-lg font-semibold">{title}</Dialog.Title>
          <Dialog.Description className="text-sm text-slate-500 dark:text-slate-300">{description}</Dialog.Description>
          <div className="mt-4 flex justify-end gap-2">
            <Dialog.Close asChild><button className="btn">Cancel</button></Dialog.Close>
            <button className="btn" onClick={onConfirm}>Confirm</button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
